/***** BEGIN LICENSE BLOCK *****
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# The Initial Developer of the Original Code is the Mozilla Foundation.
# Portions created by the Initial Developer are Copyright (C) 2014-2015
# the Initial Developer. All Rights Reserved.
#
# Contributor(s):
#   Mike Trinkala (trink@mozilla.com)
#   Rob Miller (rmiller@mozilla.com)
#   Matt Moyer (moyer@simple.com)
#
# ***** END LICENSE BLOCK *****/

package kafka

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Shopify/sarama"
	"github.com/mozilla-services/heka/logstreamer"
	"github.com/mozilla-services/heka/message"
	"github.com/mozilla-services/heka/pipeline"
	"github.com/mozilla-services/heka/plugins/tcp"
)

type KafkaOutputConfig struct {
	// Client Config
	Id                         string
	Addrs                      []string
	MetadataRetries            int    `toml:"metadata_retries"`
	WaitForElection            uint32 `toml:"wait_for_election"`
	BackgroundRefreshFrequency uint32 `toml:"background_refresh_frequency"`

	// TLS Config
	UseTls bool `toml:"use_tls"`
	Tls    tcp.TlsConfig

	// Broker Config
	MaxOpenRequests int    `toml:"max_open_reqests"`
	DialTimeout     uint32 `toml:"dial_timeout"`
	ReadTimeout     uint32 `toml:"read_timeout"`
	WriteTimeout    uint32 `toml:"write_timeout"`

	// Producer Config
	Partitioner string // Random, RoundRobin, Hash
	// Hash/Topic variables are restricted to the Type", "Logger", "Hostname" and "Payload" headers.
	// Field variables are unrestricted.
	HashVariable  string `toml:"hash_variable"`  // HashPartitioner key is extracted from a message variable
	TopicVariable string `toml:"topic_variable"` // Topic extracted from a message variable
	Topic         string // Static topic

	RequiredAcks               string `toml:"required_acks"` // NoResponse, WaitForLocal, WaitForAll
	Timeout                    uint32
	CompressionCodec           string `toml:"compression_codec"` // None, GZIP, Snappy
	MaxBufferTime              uint32 `toml:"max_buffer_time"`
	MaxBufferedBytes           uint32 `toml:"max_buffered_bytes"`
	BackPressureThresholdBytes uint32 `toml:"back_pressure_threshold_bytes"`
	MaxMessageBytes            uint32 `toml:"max_message_bytes"`

	// Action when error occurs
	OnError string `toml:"on_error"` // Discard, Retry
	// Maximum retries of sending message
	ErrorTries uint32 `toml:"error_tries"` // 0 = Inf
	// Timeout between sending errors again
	ErrorTimeout time.Duration `toml:"error_timeout"`

	// Whether to send checkpointer message after successful upload
	CreateCheckpoints bool `toml:"create_checkpoints"`
	// Interval in seconds between saving checkpoints, default is 60
	CheckpointInterval int64 `toml:"checkpoint_interval"`
	// Checkpoint base directory for saving checkpoint files
	CheckpointDirectory string // could not be set via config
}

var fieldRegex = regexp.MustCompile("^Fields\\[([^\\]]*)\\](?:\\[(\\d+)\\])?(?:\\[(\\d+)\\])?$")

type messageVariable struct {
	header bool
	name   string
	fi     int
	ai     int
}

type messageMetadata struct {
	Checkpoint struct {
		File      string
		Offset    int64
		Hash      string
		Logger    string
		Timestamp int64
	}
}

const (
	errors_discard = iota
	errors_retry
)

type state struct {
	ok       bool
	change   int64
	interval int64
}

func (s *state) Ok() bool {
	return s.ok && time.Now().Unix() > s.change+s.interval
}
func (s *state) SetOk() {
	if !s.ok {
		// change time only first time after error
		s.change = time.Now().Unix()
	}
	s.ok = true
}
func (s *state) SetError() {
	s.ok = false
	s.change = time.Now().Unix()
}

type KafkaOutput struct {
	processMessageCount    int64
	processMessageFailures int64
	processMessageDiscards int64
	kafkaDroppedMessages   int64
	kafkaEncodingErrors    int64

	hashVariable   *messageVariable
	topicVariable  *messageVariable
	config         *KafkaOutputConfig
	saramaConfig   *sarama.Config
	client         sarama.Client
	producer       sarama.AsyncProducer
	pipelineConfig *pipeline.PipelineConfig

	onError byte
	state
}

func (k *KafkaOutput) ConfigStruct() interface{} {
	hn := k.pipelineConfig.Hostname()
	baseDir := k.pipelineConfig.Globals.BaseDir
	return &KafkaOutputConfig{
		Id:                         hn,
		MetadataRetries:            3,
		WaitForElection:            250,
		BackgroundRefreshFrequency: 10 * 60 * 1000,
		MaxOpenRequests:            4,
		Timeout:                    1000,
		DialTimeout:                60 * 1000,
		ReadTimeout:                60 * 1000,
		WriteTimeout:               60 * 1000,
		Partitioner:                "Random",
		RequiredAcks:               "WaitForLocal",
		CompressionCodec:           "None",
		MaxBufferTime:              1,
		MaxBufferedBytes:           1,
		OnError:                    "Discard",
		CreateCheckpoints:          false,
		CheckpointInterval:         60,
		CheckpointDirectory:        filepath.Join(baseDir, "checkpoint"),
	}
}

func verifyMessageVariable(key string) *messageVariable {
	switch key {
	case "Type", "Logger", "Hostname", "Payload":
		return &messageVariable{header: true, name: key}
	default:
		matches := fieldRegex.FindStringSubmatch(key)
		if len(matches) == 4 {
			mvar := &messageVariable{header: false, name: matches[1]}
			if len(matches[2]) > 0 {
				if parsedInt, err := strconv.ParseInt(matches[2], 10, 32); err == nil {
					mvar.fi = int(parsedInt)
				} else {
					return nil
				}
			}
			if len(matches[3]) > 0 {
				if parsedInt, err := strconv.ParseInt(matches[3], 10, 32); err == nil {
					mvar.ai = int(parsedInt)
				} else {
					return nil
				}
			}
			return mvar
		}
		return nil
	}
}

func getFieldAsString(msg *message.Message, mvar *messageVariable) string {
	var field *message.Field
	if mvar.fi != 0 {
		fields := msg.FindAllFields(mvar.name)
		if mvar.fi >= len(fields) {
			return ""
		}
		field = fields[mvar.fi]
	} else {
		if field = msg.FindFirstField(mvar.name); field == nil {
			return ""
		}
	}
	switch field.GetValueType() {
	case message.Field_STRING:
		if mvar.ai >= len(field.ValueString) {
			return ""
		}
		return field.ValueString[mvar.ai]
	case message.Field_BYTES:
		if mvar.ai >= len(field.ValueBytes) {
			return ""
		}
		return string(field.ValueBytes[mvar.ai])
	case message.Field_INTEGER:
		if mvar.ai >= len(field.ValueInteger) {
			return ""
		}
		return fmt.Sprintf("%d", field.ValueInteger[mvar.ai])
	case message.Field_DOUBLE:
		if mvar.ai >= len(field.ValueDouble) {
			return ""
		}
		return fmt.Sprintf("%g", field.ValueDouble[mvar.ai])
	case message.Field_BOOL:
		if mvar.ai >= len(field.ValueBool) {
			return ""
		}
		return fmt.Sprintf("%t", field.ValueBool[mvar.ai])
	}
	return ""
}

func getMessageVariable(msg *message.Message, mvar *messageVariable) string {
	if mvar.header {
		switch mvar.name {
		case "Type":
			return msg.GetType()
		case "Logger":
			return msg.GetLogger()
		case "Hostname":
			return msg.GetHostname()
		case "Payload":
			return msg.GetPayload()
		default:
			return ""
		}
	} else {
		return getFieldAsString(msg, mvar)
	}
}

func (k *KafkaOutput) SetPipelineConfig(pConfig *pipeline.PipelineConfig) {
	k.pipelineConfig = pConfig
}

func (k *KafkaOutput) Init(config interface{}) (err error) {
	k.config = config.(*KafkaOutputConfig)
	if len(k.config.Addrs) == 0 {
		return errors.New("addrs must have at least one entry")
	}

	if k.config.MaxMessageBytes == 0 {
		k.config.MaxMessageBytes = message.MAX_RECORD_SIZE
	}

	switch k.config.OnError {
	case "Discard":
		k.onError = errors_discard
	case "Retry":
		k.onError = errors_retry
	default:
		return fmt.Errorf("invalid on_error: %s", k.config.OnError)
	}

	k.saramaConfig = sarama.NewConfig()
	k.saramaConfig.ClientID = k.config.Id
	k.saramaConfig.Metadata.Retry.Max = k.config.MetadataRetries
	k.saramaConfig.Metadata.Retry.Backoff = time.Duration(k.config.WaitForElection) * time.Millisecond
	k.saramaConfig.Metadata.RefreshFrequency = time.Duration(k.config.BackgroundRefreshFrequency) * time.Millisecond

	k.saramaConfig.Net.TLS.Enable = k.config.UseTls
	if k.config.UseTls {
		if k.saramaConfig.Net.TLS.Config, err = tcp.CreateGoTlsConfig(&k.config.Tls); err != nil {
			return fmt.Errorf("TLS init error: %s", err)
		}
	}

	k.saramaConfig.Net.MaxOpenRequests = k.config.MaxOpenRequests
	k.saramaConfig.Net.DialTimeout = time.Duration(k.config.DialTimeout) * time.Millisecond
	k.saramaConfig.Net.ReadTimeout = time.Duration(k.config.ReadTimeout) * time.Millisecond
	k.saramaConfig.Net.WriteTimeout = time.Duration(k.config.WriteTimeout) * time.Millisecond

	k.saramaConfig.Producer.MaxMessageBytes = int(k.config.MaxMessageBytes)
	switch k.config.Partitioner {
	case "Random":
		k.saramaConfig.Producer.Partitioner = sarama.NewRandomPartitioner
		if len(k.config.HashVariable) > 0 {
			return fmt.Errorf("hash_variable should not be set for the %s partitioner", k.config.Partitioner)
		}
	case "RoundRobin":
		k.saramaConfig.Producer.Partitioner = sarama.NewRoundRobinPartitioner
		if len(k.config.HashVariable) > 0 {
			return fmt.Errorf("hash_variable should not be set for the %s partitioner", k.config.Partitioner)
		}
	case "Hash":
		k.saramaConfig.Producer.Partitioner = sarama.NewHashPartitioner
		if k.hashVariable = verifyMessageVariable(k.config.HashVariable); k.hashVariable == nil {
			return fmt.Errorf("invalid hash_variable: %s", k.config.HashVariable)
		}
	default:
		return fmt.Errorf("invalid partitioner: %s", k.config.Partitioner)
	}

	if len(k.config.Topic) == 0 {
		if k.topicVariable = verifyMessageVariable(k.config.TopicVariable); k.topicVariable == nil {
			return fmt.Errorf("invalid topic_variable: %s", k.config.TopicVariable)
		}
	} else if len(k.config.TopicVariable) > 0 {
		return errors.New("topic and topic_variable cannot both be set")
	}

	switch k.config.RequiredAcks {
	case "NoResponse":
		k.saramaConfig.Producer.RequiredAcks = sarama.NoResponse
	case "WaitForLocal":
		k.saramaConfig.Producer.RequiredAcks = sarama.WaitForLocal
	case "WaitForAll":
		k.saramaConfig.Producer.RequiredAcks = sarama.WaitForAll
	default:
		return fmt.Errorf("invalid required_acks: %s", k.config.RequiredAcks)
	}

	k.saramaConfig.Producer.Timeout = time.Duration(k.config.Timeout) * time.Millisecond

	switch k.config.CompressionCodec {
	case "None":
		k.saramaConfig.Producer.Compression = sarama.CompressionNone
	case "GZIP":
		k.saramaConfig.Producer.Compression = sarama.CompressionGZIP
	case "Snappy":
		k.saramaConfig.Producer.Compression = sarama.CompressionSnappy
	default:
		return fmt.Errorf("invalid compression_codec: %s", k.config.CompressionCodec)
	}

	k.saramaConfig.Producer.Flush.Bytes = int(k.config.MaxBufferedBytes)
	k.saramaConfig.Producer.Flush.Frequency = time.Duration(k.config.MaxBufferTime) * time.Millisecond

	if k.config.CreateCheckpoints {
		k.saramaConfig.Producer.Return.Successes = true
		// Setup the checkpoint  dir.
		if err = os.MkdirAll(k.config.CheckpointDirectory, 0744); err != nil {
			return err
		}
	}

	k.client, err = sarama.NewClient(k.config.Addrs, k.saramaConfig)
	if err != nil {
		return err
	}
	k.producer, err = sarama.NewAsyncProducer(k.config.Addrs, k.saramaConfig)
	return err
}

func (k *KafkaOutput) processKafkaErrors(or pipeline.OutputRunner, errChan <-chan *sarama.ProducerError,
	shutdownChan chan struct{}, wg *sync.WaitGroup, retryErrorChan chan *sarama.ProducerMessage) {

	var (
		ok   = true
		pErr *sarama.ProducerError
	)
	for ok {
		select {
		case pErr, ok = <-errChan:
			if !ok {
				break
			}
			err := pErr.Err
			switch err.(type) {
			case sarama.PacketEncodingError:
				atomic.AddInt64(&k.kafkaEncodingErrors, 1)
				or.LogError(fmt.Errorf("kafka encoding error: %s", err.Error()))
			default:
				if err == nil {
					break
				}
				switch err {
				case sarama.ErrMessageSizeTooLarge:
					atomic.AddInt64(&k.kafkaDroppedMessages, 1)
					or.LogError(fmt.Errorf("kafka too large message error: %s", err.Error()))
					break
				default:
					msgValue, _ := pErr.Msg.Value.Encode()
					or.LogError(fmt.Errorf("kafka error '%s' for message '%s'", err.Error(),
						string(msgValue)))
					if retryErrorChan != nil {
						select {
						case retryErrorChan <- pErr.Msg:
							break
						case <-shutdownChan:
							ok = false
							break
						}
					} else {
						atomic.AddInt64(&k.kafkaDroppedMessages, 1)
					}
				}
			}
		case <-shutdownChan:
			ok = false
			break
		}
	}
	wg.Done()
}

type checkpoint struct {
	change    int64
	interval  int64
	File      string
	Offset    int64
	Hash      string
	Timestamp int64
}

func NewCheckpoint(interval int64) *checkpoint {
	chck := new(checkpoint)
	chck.interval = interval
	chck.Touch()
	return chck
}
func NewCheckpointFrom(src *checkpoint) *checkpoint {
	chck := new(checkpoint)
	chck.interval = src.interval
	chck.File = src.File
	chck.Offset = src.Offset
	chck.Hash = src.Hash
	chck.Timestamp = src.Timestamp
	chck.Touch()
	return chck
}
func (ch *checkpoint) Touch() {
	ch.change = time.Now().Unix()
}
func (ch *checkpoint) Old() bool {
	return time.Now().Unix() > ch.change+ch.interval
}
func (ch *checkpoint) Set(metadata *messageMetadata) {
	ch.File = metadata.Checkpoint.File
	ch.Offset = metadata.Checkpoint.Offset
	ch.Hash = metadata.Checkpoint.Hash
	ch.Timestamp = metadata.Checkpoint.Timestamp
}

type checkpointManager struct {
	current map[string]*checkpoint
	old     map[string]*checkpoint
	plugin  *KafkaOutput
	or      pipeline.OutputRunner
}

func NewCheckpointManager(k *KafkaOutput, or pipeline.OutputRunner) *checkpointManager {
	return &checkpointManager{
		current: make(map[string]*checkpoint),
		old:     make(map[string]*checkpoint),
		plugin:  k,
		or:      or,
	}
}
func (m *checkpointManager) CheckOldAll() {
	for logger := range m.old {
		m.CheckOld(logger)
	}
}
func (m *checkpointManager) CheckOld(logger string) {
	oldChck, ok := m.old[logger]
	if !ok {
		return
	}
	if !m.plugin.state.Ok() || !oldChck.Old() {
		return
	}
	// we are in OK state and old checkpoint is stable enough
	path := filepath.Join(m.plugin.config.CheckpointDirectory, logger)
	loc := new(logstreamer.LogstreamLocation)
	loc.Reset() // init
	loc.SeekPosition = oldChck.Offset
	loc.Filename = oldChck.File
	loc.Hash = oldChck.Hash

	// save
	locBytes, err := json.Marshal(loc)
	if err != nil {
		m.or.LogError(fmt.Errorf("kafka checkpoint error: %s", err.Error()))
		return
	}
	chckFile, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0660)
	if err != nil {
		m.or.LogError(fmt.Errorf("kafka checkpoint error: %s", err.Error()))
		return
	}
	defer chckFile.Close()
	_, err = chckFile.Write(locBytes)
	if err != nil {
		m.or.LogError(fmt.Errorf("kafka checkpoint error: %s", err.Error()))
		return
	}

	oldChck.Touch()
}
func (m *checkpointManager) getCurrent(logger string) *checkpoint {
	curChck, ok := m.current[logger]
	if !ok {
		curChck = NewCheckpoint(m.plugin.config.CheckpointInterval)
		m.current[logger] = curChck
	}
	return curChck
}
func (m *checkpointManager) CheckCurrentAll() {
	for logger := range m.current {
		m.CheckCurrent(logger)
	}
}
func (m *checkpointManager) CheckCurrent(logger string) {
	curChck := m.getCurrent(logger)
	if m.plugin.state.Ok() && curChck.Old() {
		// we are in OK state and current checkpoint is stable enough
		curChck.Touch()
		m.old[logger] = curChck
		curChck = NewCheckpointFrom(curChck) // copy
		m.current[logger] = curChck
	}
}
func (m *checkpointManager) UpdateCurrent(metadata *messageMetadata) {
	curChck := m.getCurrent(metadata.Checkpoint.Logger)
	if metadata.Checkpoint.Timestamp > curChck.Timestamp {
		// found new checkpoint
		curChck.Set(metadata)
	}
}

func (k *KafkaOutput) processKafkaSuccess(or pipeline.OutputRunner,
	sucChan <-chan *sarama.ProducerMessage, shutdownChan chan struct{},
	wg *sync.WaitGroup) {

	var (
		metadata *messageMetadata
		run      = true
		pMsg     *sarama.ProducerMessage
		ticker   = time.NewTicker(time.Second * time.Duration(k.config.CheckpointInterval))
		chckMan  = NewCheckpointManager(k, or)
		ok       bool
	)

	for run {
		select {
		case pMsg, run = <-sucChan:
			if !run {
				break
			}
			if metadata, ok = pMsg.Metadata.(*messageMetadata); !ok {
				continue
			}
			chckMan.CheckOld(metadata.Checkpoint.Logger)
			chckMan.CheckCurrent(metadata.Checkpoint.Logger)
			chckMan.UpdateCurrent(metadata)
		case <-ticker.C:
			// once per ticker interval update all old and current checkpoints
			chckMan.CheckOldAll()
			chckMan.CheckCurrentAll()
		case <-shutdownChan:
			run = false
			break
		}
	}
	ticker.Stop()
	wg.Done()
}

func (k *KafkaOutput) Run(or pipeline.OutputRunner, h pipeline.PluginHelper) (err error) {
	defer func() {
		k.producer.Close()
		k.client.Close()
	}()

	if or.Encoder() == nil {
		return errors.New("Encoder required.")
	}

	k.state.interval = k.config.CheckpointInterval
	inChan := or.InChan()
	errChan := k.producer.Errors()
	pInChan := k.producer.Input()
	shutdownChan := make(chan struct{})
	var retryErrorChan chan *sarama.ProducerMessage
	var syncProducer sarama.SyncProducer
	if k.onError != errors_discard {
		retryErrorChan = make(chan *sarama.ProducerMessage)
		syncConf := *k.saramaConfig
		syncProducer, err = sarama.NewSyncProducer(k.config.Addrs, &syncConf)
		if err != nil {
			return err
		}
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go k.processKafkaErrors(or, errChan, shutdownChan, &wg, retryErrorChan)

	var successShutdownChan chan struct{}
	if k.config.CreateCheckpoints {
		successShutdownChan = make(chan struct{})
		wg.Add(1)
		go k.processKafkaSuccess(or, k.producer.Successes(),
			successShutdownChan, &wg)
	}

	var (
		pack  *pipeline.PipelinePack
		topic = k.config.Topic
		key   sarama.Encoder

		file, hash, logger string
		offset, timestamp  float64
		fileField          = verifyMessageVariable("Fields[checkpoint_file]")
		offsetField        = verifyMessageVariable("Fields[checkpoint_offset]")
		hashField          = verifyMessageVariable("Fields[checkpoint_hash]")
		timestampField     = verifyMessageVariable("Fields[checkpoint_timestamp]")
		loggerField        = verifyMessageVariable("Fields[checkpoint_logger]")
	)

	for pack = range inChan {
		atomic.AddInt64(&k.processMessageCount, 1)

		if k.topicVariable != nil {
			topic = getMessageVariable(pack.Message, k.topicVariable)
		}
		if k.hashVariable != nil {
			key = sarama.StringEncoder(getMessageVariable(pack.Message, k.hashVariable))
		}

		msgBytes, err := or.Encode(pack)
		if err != nil {
			atomic.AddInt64(&k.processMessageFailures, 1)
			or.LogError(err)
			// Don't retry encoding errors.
			or.UpdateCursor(pack.QueueCursor)
			pack.Recycle(nil)
			continue
		}
		if msgBytes == nil {
			atomic.AddInt64(&k.processMessageDiscards, 1)
			or.UpdateCursor(pack.QueueCursor)
			pack.Recycle(nil)
			continue
		}
		pMessage := &sarama.ProducerMessage{
			Topic: topic,
			Key:   key,
			Value: sarama.ByteEncoder(msgBytes),
		}
		if k.config.CreateCheckpoints {
			file = getFieldAsString(pack.Message, fileField)
			offset, _ = strconv.ParseFloat(
				getFieldAsString(pack.Message, offsetField), 64)
			hash = getFieldAsString(pack.Message, hashField)
			timestamp, _ = strconv.ParseFloat(
				getFieldAsString(pack.Message, timestampField), 64)
			logger = getFieldAsString(pack.Message, loggerField)

			metadata := &messageMetadata{}
			metadata.Checkpoint.File = file
			metadata.Checkpoint.Offset = int64(offset)
			metadata.Checkpoint.Hash = hash
			metadata.Checkpoint.Timestamp = int64(timestamp)
			metadata.Checkpoint.Logger = logger
			pMessage.Metadata = metadata
		}
		pInChan <- pMessage

		// retry errors
		if retryErrorChan != nil {
			for tryAgain := true; tryAgain; {
				tryAgain = k.retryError(or, syncProducer, retryErrorChan)
			}
		}
		pack.Recycle(nil)
		k.state.SetOk()
	}

	close(shutdownChan)
	if successShutdownChan != nil {
		close(successShutdownChan)
	}
	wg.Wait()
	return
}

func (k *KafkaOutput) retryError(or pipeline.OutputRunner,
	syncProducer sarama.SyncProducer, retryErrorChan chan *sarama.ProducerMessage) bool {

	select {
	case pmsg := <-retryErrorChan:
		for i := uint32(0); k.config.ErrorTries <= 0 || i < k.config.ErrorTries; i++ {
			k.state.SetError()

			if k.pipelineConfig.Globals.IsShuttingDown() {
				atomic.AddInt64(&k.kafkaDroppedMessages, 1)
				or.LogMessage("Stop trying - shuting down...")
				return false
			}

			if _, _, err := syncProducer.SendMessage(pmsg); err == nil {
				or.LogMessage(fmt.Sprintf("message finally sent"))
				return true
			}
			or.LogMessage(fmt.Sprintf("message retry %v", i+1))
			time.Sleep(k.config.ErrorTimeout * time.Millisecond)
		}
		atomic.AddInt64(&k.kafkaDroppedMessages, 1)
		or.LogMessage(fmt.Sprintf("no more tries, message discarded"))
		return true
	default:
	}
	return false
}

func (k *KafkaOutput) ReportMsg(msg *message.Message) error {
	message.NewInt64Field(msg, "ProcessMessageCount",
		atomic.LoadInt64(&k.processMessageCount), "count")
	message.NewInt64Field(msg, "ProcessMessageFailures",
		atomic.LoadInt64(&k.processMessageFailures), "count")
	message.NewInt64Field(msg, "ProcessMessageDiscards",
		atomic.LoadInt64(&k.processMessageDiscards), "count")
	message.NewInt64Field(msg, "KafkaDroppedMessages",
		atomic.LoadInt64(&k.kafkaDroppedMessages), "count")
	message.NewInt64Field(msg, "KafkaEncodingErrors",
		atomic.LoadInt64(&k.kafkaEncodingErrors), "count")
	return nil
}

func (k *KafkaOutput) CleanupForRestart() {
	return
}

func init() {
	pipeline.RegisterPlugin("KafkaOutput", func() interface{} {
		return new(KafkaOutput)
	})
}
