// Unit tests for the service layer.
package test

import (
	"context"
	"errors"
	"testing"

	"flowguard/internal/model"
	"flowguard/internal/service"
)

var errKafkaFailed = errors.New("broker unavailable")

func TestSendMessageServiceEcho(t *testing.T) {
	stub := &stubProducer{topic: "RECONCILIATION"}
	svc := service.NewSendMessageService("Hello", "RECONCILIATION", stub)
	resp, err := svc.Send(context.Background(), model.SendMessageRequest{Message: "world"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Echo != "Hello from world" {
		t.Fatalf("echo = %q, want %q", resp.Echo, "Hello from world")
	}
	if len(stub.recorded) != 1 {
		t.Fatalf("expected 1 publish call, got %d", len(stub.recorded))
	}
	c := stub.recorded[0]
	if c.topic != "RECONCILIATION" {
		t.Fatalf("topic = %q, want RECONCILIATION", c.topic)
	}
	if c.key != "world" {
		t.Fatalf("key = %q, want world", c.key)
	}
	if c.value != "world" {
		t.Fatalf("value = %q, want world", c.value)
	}
	if resp.ReceivedAt.IsZero() {
		t.Fatal("missing ReceivedAt")
	}
}

func TestSendMessageServiceCustomGreeting(t *testing.T) {
	stub := &stubProducer{}
	svc := service.NewSendMessageService("Hi", "RECONCILIATION", stub)
	resp, err := svc.Send(context.Background(), model.SendMessageRequest{Message: "there"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Echo != "Hi from there" {
		t.Fatalf("echo = %q, want %q", resp.Echo, "Hi from there")
	}
}

func TestSendMessageServiceDefaultGreeting(t *testing.T) {
	stub := &stubProducer{}
	svc := service.NewSendMessageService("", "RECONCILIATION", stub)
	resp, err := svc.Send(context.Background(), model.SendMessageRequest{Message: "x"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Echo != "Hello from x" {
		t.Fatalf("echo = %q, want %q", resp.Echo, "Hello from x")
	}
}

func TestSendMessageServicePublishFailure(t *testing.T) {
	stub := &stubProducer{err: errKafkaFailed}
	svc := service.NewSendMessageService("Hello", "RECONCILIATION", stub)
	resp, err := svc.Send(context.Background(), model.SendMessageRequest{Message: "x"})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if resp.Echo != "" {
		t.Fatalf("expected empty response, got %q", resp.Echo)
	}
}
