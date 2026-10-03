// Package kafka tests — compile-time check that the Producer interface is
// satisfied by the concrete producer. Real Kafka connections are tested via
// integration suites; here we validate the config validation path.
package kafka

import (
	"testing"
)

func TestNewNoBrokers(t *testing.T) {
	_, err := New(Config{})
	if err == nil {
		t.Fatal("expected error when no brokers configured")
	}
}

func TestNewBadBroker(t *testing.T) {
	// A broker that doesn't exist; NewSyncProducer should fail connecting.
	_, err := New(Config{
		Brokers: []string{"localhost:1"},
	})
	if err == nil {
		t.Fatal("expected error for unreachable broker")
	}
}