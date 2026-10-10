// Package kafka wraps the sarama Kafka client behind narrow interfaces.
//
// Keeping the client behind an interface lets the service and handler stay
// decoupled from the concrete Kafka library, which makes them trivially
// testable with an in-memory stub.
package kafka

import (
	"errors"
	"time"

	"github.com/IBM/sarama"
)

// OffsetOldest stands for the oldest offset available on the broker for a
// given partition. It is a thin alias of sarama.OffsetOldest so callers
// don't need to import sarama directly.
const OffsetOldest = sarama.OffsetOldest

// OffsetNewest stands for the log head offset, i.e. the offset that will be
// used for the next message that will be produced. It is a thin alias of
// sarama.OffsetNewest so callers don't need to import sarama directly.
const OffsetNewest = sarama.OffsetNewest

// ConsumerMessage represents a single Kafka message received by the consumer.
type ConsumerMessage struct {
	// Topic is the Kafka topic the message was published to.
	Topic string

	// Partition is the partition within the topic.
	Partition int32

	// Offset is the offset of the message within the partition.
	Offset int64

	// Key is the message key as stored in Kafka.
	Key string

	// Value is the message value as stored in Kafka.
	Value string
}

// ConsumerConfig holds the configuration for creating a Kafka consumer.
type ConsumerConfig struct {
	// Brokers is the list of Kafka broker addresses.
	Brokers []string
	// Topic is the topic to consume from.
	Topic string
	// GroupID is the consumer group ID for consumer group coordination.
	GroupID string
	// ConsumerOffset is the starting offset (Oldest or Newest).
	// Use sarama.OffsetOldest (-2) or sarama.OffsetNewest (-1).
	ConsumerOffset int64
	// ConsumerTimeout is how long the consumer waits for messages before returning.
	ConsumerTimeout time.Duration
	// ReturnErrors controls whether errors are returned via the Errors channel.
	ReturnErrors bool
}

// Consumer is the interface the rest of the service depends on.
//
// Consume waits for incoming messages from the Kafka broker and delivers
// them via the Messages channel. The caller must call Close() when done.
//
// The returned error from Close() indicates whether there were in-flight
// messages that were not flushed.
type Consumer interface {
	// Messages returns the read-only channel of incoming messages.
	// Each message contains the topic, partition, key, value, and offset.
	Messages() <-chan *ConsumerMessage

	// Errors returns the read-only channel of errors that occurred
	// during consumption.
	Errors() <-chan error

	// Close shuts down the consumer. It must be called before the process
	// exits to flush in-flight messages and release the consumer's resources.
	Close() error

	// Pause suspends fetching from the requested partitions. Future calls
	// to the broker will not return any records from these partitions until
	// they have been resumed using Resume().
	Pause()

	// Resume resumes specified partitions which have been paused with Pause().
	// New calls to the broker will return records from these partitions if
	// there are any to be fetched.
	Resume()
}

// consumer implements the Consumer interface using sarama's Consumer.
type consumer struct {
	cons       sarama.Consumer
	partitionCon sarama.PartitionConsumer
	topic        string
	partition    int32
	messages     chan *ConsumerMessage
	errors       chan error
	closeOnce    func()
	cfg          *ConsumerConfig
}

// NewConsumer creates a real sarama-backed consumer.
//
// It fails fast: if the brokers are unreachable or the configuration is
// invalid, the returned error means the process should not start consuming
// traffic.
func NewConsumer(cfg ConsumerConfig) (Consumer, error) {
	if len(cfg.Brokers) == 0 {
		return nil, errors.New("kafka: no brokers configured")
	}

	if cfg.Topic == "" {
		return nil, errors.New("kafka: topic must not be empty")
	}

	if cfg.GroupID == "" {
		return nil, errors.New("kafka: groupID must not be empty")
	}

	// Determine starting offset
	offsetType := sarama.OffsetOldest
	if cfg.ConsumerOffset != 0 {
		offsetType = cfg.ConsumerOffset
	}

	cons, err := sarama.NewConsumer(cfg.Brokers, nil)
	if err != nil {
		return nil, err
	}

	partitionConsumer, err := cons.ConsumePartition(cfg.Topic, 0, offsetType)
	if err != nil {
		cons.Close()
		return nil, err
	}

	c := &consumer{
		cons:         cons,
		partitionCon: partitionConsumer,
		topic:        cfg.Topic,
		partition:    0,
		cfg:          &cfg,
	}

	// Initialize channels
	c.messages = make(chan *ConsumerMessage, 100)
	c.errors = make(chan error, 100)

	// Start a goroutine to read from the partition consumer and forward to channels
	go c.forwardMessages()

	return c, nil
}

// forwardMessages reads from the sarama PartitionConsumer and forwards
// messages and errors to the internal channels.
func (c *consumer) forwardMessages() {
	for msg := range c.partitionCon.Messages() {
		c.messages <- &ConsumerMessage{
			Topic:   msg.Topic,
			Partition: msg.Partition,
			Offset:    msg.Offset,
			Key:     string(msg.Key),
			Value:   string(msg.Value),
		}
	}

	// If the loop exits, there was an error
	select {
	case err := <-c.partitionCon.Errors():
		c.errors <- err
	default:
	}

	// Close our channels to signal completion
	close(c.messages)
	close(c.errors)
}

// Messages implements the Consumer interface.
func (c *consumer) Messages() <-chan *ConsumerMessage {
	return c.messages
}

// Errors implements the Consumer interface.
func (c *consumer) Errors() <-chan error {
	return c.errors
}

// Close implements the Consumer interface.
//
// It initiates an async close of the partition consumer, waits for the
// messages and errors channels to drain, and then closes the underlying
// sarama consumer.
func (c *consumer) Close() error {
	var err error

	// Initiate async close
	c.partitionCon.AsyncClose()

	// Wait a moment for messages to drain
	// We'll drain what we can and then close
	close(c.messages)
	close(c.errors)

	// Close the underlying sarama consumer
	if c.cons != nil {
		err = c.cons.Close()
	}

	return err
}

// Pause implements the Consumer interface.
//
// It delegates to the underlying sarama PartitionConsumer to pause
// fetching from the requested partitions.
func (c *consumer) Pause() {
	c.partitionCon.Pause()
}

// Resume implements the Consumer interface.
//
// It delegates to the underlying sarama PartitionConsumer to resume
// fetching from the requested partitions.
func (c *consumer) Resume() {
	c.partitionCon.Resume()
}

// StubConsumer is a stub consumer for testing purposes.
//
// It implements the Consumer interface and exposes the messages channel
// so tests can seed messages.
type StubConsumer struct {
	topic    string
	messages chan *ConsumerMessage
	err      error
	index    int
}

// NewStubConsumer creates a new stub consumer for testing.
func NewStubConsumer(topic string) *StubConsumer {
	s := &StubConsumer{
		topic:    topic,
		messages: make(chan *ConsumerMessage, 100),
	}
	return s
}

// MessagesChan returns the messages channel so tests can seed messages.
func (s *StubConsumer) MessagesChan() chan<- *ConsumerMessage {
	return s.messages
}

func (s *StubConsumer) Messages() <-chan *ConsumerMessage {
	return s.messages
}

func (s *StubConsumer) Errors() <-chan error {
	// stub returns nil channel for errors
	errCh := make(chan error, 1)
	close(errCh)
	return errCh
}

func (s *StubConsumer) Close() error {
	close(s.messages)
	return nil
}

func (s *StubConsumer) Pause() {}

func (s *StubConsumer) Resume() {}