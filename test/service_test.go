// Unit tests for the service layer.
package test

import (
	"testing"

	"flowguard/internal/model"
	"flowguard/internal/service"
)

func TestSendMessageServiceEcho(t *testing.T) {
	svc := service.NewSendMessageService("Hello")
	resp, err := svc.Send(model.SendMessageRequest{Message: "world"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Echo != "Hello from world" {
		t.Fatalf("echo = %q, want %q", resp.Echo, "Hello from world")
	}
	if resp.ReceivedAt.IsZero() {
		t.Fatal("missing ReceivedAt")
	}
}

func TestSendMessageServiceCustomGreeting(t *testing.T) {
	svc := service.NewSendMessageService("Hi")
	resp, err := svc.Send(model.SendMessageRequest{Message: "there"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Echo != "Hi from there" {
		t.Fatalf("echo = %q, want %q", resp.Echo, "Hi from there")
	}
}

func TestSendMessageServiceDefaultGreeting(t *testing.T) {
	svc := service.NewSendMessageService("")
	resp, err := svc.Send(model.SendMessageRequest{Message: "x"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Echo != "Hello from x" {
		t.Fatalf("echo = %q, want %q", resp.Echo, "Hello from x")
	}
}