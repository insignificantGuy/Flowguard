// Package kafka wraps the sarama Kafka client behind a narrow interface.
//
// Keeping the client behind an interface lets the service and handler stay
// decoupled from the concrete Kafka library, which makes them trivially
// testable with an in-memory stub.
package kafka

import (
	"context"
	"errors"
	"time"

	"github.com/IBM/sarama"

	"flowguard/internal/logger"
)

// Producer is the interface the rest of the service depends on.
//
// Publish waits for the broker ack before returning — the caller (the
// send-message handler) treats a returned error as a request failure.
type Producer interface {
	// Publish writes a record to the given topic with the given key and value.
	// It returns only after the broker acknowledges the write, or with an
	// error if the write failed or the context was cancelled.
	Publish(ctx context.Context, topic, key, value string) error

	// Close flushes in-flight messages and releases the producer's resources.
	// Must be called before the process exits.
	Close() error
}

// Config drives producer construction.
type Config struct {
	// Brokers is the list of Kafka broker addresses.
	Brokers []string
	// Timeout is how long Publish waits for a broker ack.
	Timeout time.Duration
}

// New creates a real sarama-backed producer.
//
// It fails fast: if the brokers are unreachable or the configuration is
// invalid, the returned error means the process should not start serving
// traffic (a misconfigured deployment must never silently accept writes).
func New(c Config) (Producer, error) {
	if len(c.Brokers) == 0 {
		return nil, errors.New("kafka: no brokers configured")
	}

	cfg := sarama.NewConfig()
	cfg.Producer.Return.Successes = true
	cfg.Producer.Return.Errors = true
	cfg.Producer.RequiredAcks = sarama.WaitForAll // acknowledge replicated writes
	cfg.Producer.Retry.Max = 5
	if c.Timeout > 0 {
		cfg.Producer.Timeout = c.Timeout
	}

	p, err := sarama.NewSyncProducer(c.Brokers, cfg)
	if err != nil {
		return nil, err
	}

	return &producer{p: p, timeout: c.Timeout}, nil
}

type producer struct {
	p       sarama.SyncProducer
	timeout time.Duration
}

func (pr *producer) Publish(ctx context.Context, topic, key, value string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if pr.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, pr.timeout)
		defer cancel()
	}
	logger.Log.Info().
		Str("topic", topic).
		Str("key", key).
		Str("value", value).
		Msg("kafka publish")

	msg := &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.StringEncoder(key),
		Value: sarama.StringEncoder(value),
	}

	// Publish blocks until the broker acks or the context expires.
	if _, _, err := pr.p.SendMessage(msg); err != nil {
		logger.Log.Warn().
			Str("topic", topic).
			Str("key", key).
			Err(err).
			Msg("kafka publish failed")
		return err
	}
	return nil
}

func (pr *producer) Close() error {
	return pr.p.Close()
}
