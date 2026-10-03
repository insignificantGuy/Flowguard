// Package service holds business logic.
package service

import (
	"context"
	"time"

	"flowguard/internal/kafka"
	"flowguard/internal/model"
)

// SendMessageService handles the send-message use case.
//
// The service is the seam between the HTTP layer and Kafka: it is stateless,
// depends only on the kafka.Producer interface (so it is trivially testable
// with a stub), and is horizontally scalable by construction.
type SendMessageService struct {
	greeting string
	producer kafka.Producer
	topic    string
}

// NewSendMessageService creates a service with the given greeting, producer,
// and Kafka topic.
func NewSendMessageService(greeting, topic string, producer kafka.Producer) *SendMessageService {
	if greeting == "" {
		greeting = "Hello"
	}
	if topic == "" {
		topic = "RECONCILIATION"
	}
	return &SendMessageService{greeting: greeting, producer: producer, topic: topic}
}

// Send publishes the message to Kafka and returns the acknowledgement.
//
// The Kafka key is the message itself (per the PRD); the value is the raw
// message string. The publish waits for the broker ack, so a returned error
// means the message was NOT durably written.
func (s *SendMessageService) Send(ctx context.Context, req model.SendMessageRequest) (model.SendMessageResponse, error) {
	if err := s.producer.Publish(ctx, s.topic, req.Message, req.Message); err != nil {
		return model.SendMessageResponse{}, err
	}
	return model.SendMessageResponse{
		Echo:       s.greeting + " from " + req.Message,
		ReceivedAt: time.Now().UTC(),
	}, nil
}
