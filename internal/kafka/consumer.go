// Package kafka adapts Kafka consumer groups behind a testable interface.
package kafka

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/IBM/sarama"
)

const OffsetOldest = sarama.OffsetOldest
const OffsetNewest = sarama.OffsetNewest

type ConsumerMessage struct {
	Topic      string
	Partition  int32
	Offset     int64
	Key, Value string
	// Ack marks processing complete. Offsets advance only through contiguous
	// completed records in the current consumer-group session.
	Ack func()
}
type ConsumerConfig struct {
	Brokers         []string
	Topic, GroupID  string
	ConsumerOffset  int64
	ConsumerTimeout time.Duration
	ReturnErrors    bool
}
type Consumer interface {
	Messages() <-chan *ConsumerMessage
	Errors() <-chan error
	Close() error
	Pause()
	Resume()
}
type consumer struct {
	group     sarama.ConsumerGroup
	messages  chan *ConsumerMessage
	errors    chan error
	cancel    context.CancelFunc
	done      chan struct{}
	once      sync.Once
	closeErr  error
	ready     chan struct{}
	readyOnce sync.Once
}

func NewConsumer(cfg ConsumerConfig) (Consumer, error) {
	if len(cfg.Brokers) == 0 || cfg.Topic == "" || cfg.GroupID == "" {
		return nil, errors.New("kafka: brokers, topic and groupID are required")
	}
	conf := sarama.NewConfig()
	conf.Consumer.Return.Errors = false
	conf.ChannelBufferSize = 1
	conf.Consumer.Offsets.Initial = OffsetOldest
	if cfg.ConsumerOffset != 0 {
		conf.Consumer.Offsets.Initial = cfg.ConsumerOffset
	}
	group, err := sarama.NewConsumerGroup(cfg.Brokers, cfg.GroupID, conf)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	c := &consumer{group: group, messages: make(chan *ConsumerMessage), errors: make(chan error, 1), cancel: cancel, done: make(chan struct{}), ready: make(chan struct{})}
	go func() {
		defer close(c.done)
		defer close(c.messages)
		defer close(c.errors)
		for ctx.Err() == nil {
			if err := group.Consume(ctx, []string{cfg.Topic}, c); err != nil && ctx.Err() == nil {
				select {
				case c.errors <- err:
				case <-ctx.Done():
				}
				return
			}
		}
	}()

	timeout := cfg.ConsumerTimeout
	if timeout <= 0 {
		timeout = 20 * time.Second
	}
	select {
	case <-c.ready:
		return c, nil
	case <-time.After(timeout):
		c.Close()
		return nil, errors.New("kafka: consumer group assignment timed out")
	}
}
func (c *consumer) Setup(s sarama.ConsumerGroupSession) error {
	c.readyOnce.Do(func() { close(c.ready) })
	return nil
}
func (c *consumer) Cleanup(s sarama.ConsumerGroupSession) error { return nil }
func (c *consumer) ConsumeClaim(s sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	// Local sequence numbers avoid assuming Kafka offsets are gap-free.
	var mu sync.Mutex
	next, issued := int64(0), int64(0)
	completed := map[int64]*sarama.ConsumerMessage{}
	for {
		select {
		case <-s.Context().Done():
			return nil
		case m, ok := <-claim.Messages():
			if !ok {
				return nil
			}
			seq := issued
			issued++
			var once sync.Once
			msg := &ConsumerMessage{Topic: m.Topic, Partition: m.Partition, Offset: m.Offset, Key: string(m.Key), Value: string(m.Value)}
			msg.Ack = func() {
				once.Do(func() {
					mu.Lock()
					defer mu.Unlock()
					if s.Context().Err() != nil {
						return
					}
					completed[seq] = m
					for {
						record, ok := completed[next]
						if !ok {
							break
						}
						s.MarkMessage(record, "")
						delete(completed, next)
						next++
					}
				})
			}
			select {
			case c.messages <- msg:
			case <-s.Context().Done():
				return nil
			}
		}
	}
}
func (c *consumer) Messages() <-chan *ConsumerMessage { return c.messages }
func (c *consumer) Errors() <-chan error              { return c.errors }
func (c *consumer) Pause()                            { c.group.PauseAll() }
func (c *consumer) Resume()                           { c.group.ResumeAll() }
func (c *consumer) Close() error {
	c.once.Do(func() { c.cancel(); <-c.done; c.closeErr = c.group.Close() })
	return c.closeErr
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
	return nil
}

func (s *StubConsumer) Close() error {
	return nil
}

func (s *StubConsumer) Pause() {}

func (s *StubConsumer) Resume() {}
