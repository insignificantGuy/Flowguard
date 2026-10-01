// Package service holds business logic.
package service

import (
	"time"

	"flowguard/internal/model"
)

// SendMessageService handles the send-message use case.
//
// A real implementation would fan the message out to a queue / downstream
// service. Here it is a pure, deterministic function so it is trivially
// testable and horizontally scalable (stateless — no shared memory).
type SendMessageService struct {
	greeting string
}

// NewSendMessageService creates a service with the given greeting.
func NewSendMessageService(greeting string) *SendMessageService {
	if greeting == "" {
		greeting = "Hello"
	}
	return &SendMessageService{greeting: greeting}
}

// Send echoes the greeting combined with the provided message.
func (s *SendMessageService) Send(req model.SendMessageRequest) (model.SendMessageResponse, error) {
	return model.SendMessageResponse{
		Echo:       s.greeting + " from " + req.Message,
		ReceivedAt: time.Now().UTC(),
	}, nil
}