package kafka

import (
	"context"
	"github.com/IBM/sarama"
	"sync"
	"testing"
)

type fakeSession struct {
	ctx    context.Context
	mu     sync.Mutex
	marked []int64
}

func (s *fakeSession) Claims() map[string][]int32               { return nil }
func (s *fakeSession) MemberID() string                         { return "test" }
func (s *fakeSession) GenerationID() int32                      { return 1 }
func (s *fakeSession) MarkOffset(string, int32, int64, string)  {}
func (s *fakeSession) Commit()                                  {}
func (s *fakeSession) ResetOffset(string, int32, int64, string) {}
func (s *fakeSession) Context() context.Context                 { return s.ctx }
func (s *fakeSession) MarkMessage(m *sarama.ConsumerMessage, _ string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.marked = append(s.marked, m.Offset)
}

type fakeClaim struct{ messages chan *sarama.ConsumerMessage }

func (c *fakeClaim) Topic() string                            { return "test" }
func (c *fakeClaim) Partition() int32                         { return 0 }
func (c *fakeClaim) InitialOffset() int64                     { return 10 }
func (c *fakeClaim) HighWaterMarkOffset() int64               { return 13 }
func (c *fakeClaim) Messages() <-chan *sarama.ConsumerMessage { return c.messages }
func TestConsumerAckAdvancesOnlyContiguousCompletions(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := &fakeSession{ctx: ctx}
	claim := &fakeClaim{messages: make(chan *sarama.ConsumerMessage, 2)}
	claim.messages <- &sarama.ConsumerMessage{Offset: 10}
	claim.messages <- &sarama.ConsumerMessage{Offset: 12}
	c := &consumer{messages: make(chan *ConsumerMessage, 2)}
	done := make(chan error, 1)
	go func() { done <- c.ConsumeClaim(s, claim) }()
	first, second := <-c.messages, <-c.messages
	second.Ack()
	if len(s.marked) != 0 {
		t.Fatal("committed past unfinished message")
	}
	first.Ack()
	first.Ack()
	if len(s.marked) != 2 || s.marked[0] != 10 || s.marked[1] != 12 {
		t.Fatalf("marks %v", s.marked)
	}
	cancel()
	<-done
}
func TestConsumerAckIgnoresExpiredSession(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	s := &fakeSession{ctx: ctx}
	claim := &fakeClaim{messages: make(chan *sarama.ConsumerMessage, 1)}
	claim.messages <- &sarama.ConsumerMessage{Offset: 10}
	c := &consumer{messages: make(chan *ConsumerMessage, 1)}
	done := make(chan error, 1)
	go func() { done <- c.ConsumeClaim(s, claim) }()
	msg := <-c.messages
	cancel()
	<-done
	msg.Ack()
	if len(s.marked) != 0 {
		t.Fatal("ack from expired session")
	}
}
