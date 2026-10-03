// Test suite for the flowguard API.
package test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"flowguard/internal/config"
	"flowguard/internal/router"
)

// stubProducer is a kafka.Producer that records calls or fails, with no broker.
type stubProducer struct {
	topic    string
	recorded []publishCall
	err      error
}

type publishCall struct {
	topic, key, value string
}

func (s *stubProducer) Publish(_ context.Context, topic, key, value string) error {
	s.recorded = append(s.recorded, publishCall{topic, key, value})
	return s.err
}
func (s *stubProducer) Close() error { return nil }

func newTestServer(t *testing.T, producer *stubProducer) *httptest.Server {
	t.Helper()
	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("config: %v", err)
	}
	cfg.Env = "development"
	cfg.RateLimitMax = 10000             // effectively disabled for tests
	cfg.KafkaBrokers = []string{"dummy"} // so newProducer doesn't disable it; we inject our own
	app, err := router.BuildWithProducer(cfg, producer)
	if err != nil {
		t.Fatalf("router build: %v", err)
	}
	return httptest.NewServer(app)
}

func TestSendMessage(t *testing.T) {
	producer := &stubProducer{topic: "RECONCILIATION"}
	srv := newTestServer(t, producer)
	defer srv.Close()

	resp, err := http.Post(srv.URL+"/v1/send-message",
		"application/json", strings.NewReader(`{"message":"world"}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		t.Fatalf("content-type = %q", ct)
	}
	if resp.Header.Get("X-Request-Id") == "" {
		t.Fatal("missing X-Request-Id header")
	}

	var body struct {
		Data struct {
			Echo       string `json:"echo"`
			ReceivedAt string `json:"receivedAt"`
		} `json:"data"`
		TraceID   string `json:"traceId"`
		Timestamp string `json:"timestamp"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if body.Data.Echo != "Hello from world" {
		t.Fatalf("echo = %q, want %q", body.Data.Echo, "Hello from world")
	}
	if body.TraceID != resp.Header.Get("X-Request-Id") {
		t.Fatalf("traceId = %q, want %q", body.TraceID, resp.Header.Get("X-Request-Id"))
	}
	if body.Data.ReceivedAt == "" {
		t.Fatal("missing receivedAt")
	}
	if body.Timestamp == "" {
		t.Fatal("missing timestamp")
	}

	// The message must have been published to Kafka with the message as both
	// key and value.
	if len(producer.recorded) != 1 {
		t.Fatalf("expected 1 publish call, got %d", len(producer.recorded))
	}
	pub := producer.recorded[0]
	if pub.topic != "RECONCILIATION" {
		t.Fatalf("topic = %q, want RECONCILIATION", pub.topic)
	}
	if pub.key != "world" {
		t.Fatalf("key = %q, want world", pub.key)
	}
	if pub.value != "world" {
		t.Fatalf("value = %q, want world", pub.value)
	}
}

func TestSendMessageTraceIdHeader(t *testing.T) {
	srv := newTestServer(t, &stubProducer{})
	defer srv.Close()

	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/v1/send-message",
		bytes.NewBufferString(`{"message":"x"}`))
	req.Header.Set("X-Request-Id", "abc-123")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.Header.Get("X-Request-Id") != "abc-123" {
		t.Fatalf("X-Request-Id = %q, want abc-123", resp.Header.Get("X-Request-Id"))
	}
}

func TestSendMessageEmptyBody(t *testing.T) {
	srv := newTestServer(t, &stubProducer{})
	defer srv.Close()

	resp, err := http.Post(srv.URL+"/v1/send-message",
		"application/json", strings.NewReader(`{"message":""}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
	var body struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if body.Error.Code != "VALIDATION_ERROR" {
		t.Fatalf("code = %q, want VALIDATION_ERROR", body.Error.Code)
	}
}

func TestSendMessageMissingMessage(t *testing.T) {
	srv := newTestServer(t, &stubProducer{})
	defer srv.Close()

	resp, err := http.Post(srv.URL+"/v1/send-message",
		"application/json", strings.NewReader(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
}

func TestSendMessageKafkaFailure(t *testing.T) {
	producer := &stubProducer{err: errKafkaFailed}
	srv := newTestServer(t, producer)
	defer srv.Close()

	resp, err := http.Post(srv.URL+"/v1/send-message",
		"application/json", strings.NewReader(`{"message":"world"}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", resp.StatusCode)
	}
	var body struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if body.Error.Code != "SERVICE_UNAVAILABLE" {
		t.Fatalf("code = %q, want SERVICE_UNAVAILABLE", body.Error.Code)
	}
}

func TestNotFound(t *testing.T) {
	srv := newTestServer(t, &stubProducer{})
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/nope")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", resp.StatusCode)
	}
	var body struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if body.Error.Code != "NOT_FOUND" {
		t.Fatalf("code = %q, want NOT_FOUND", body.Error.Code)
	}
}

func TestHealth(t *testing.T) {
	srv := newTestServer(t, &stubProducer{})
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/health")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var body struct {
		Data struct {
			Status string `json:"status"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if body.Data.Status != "ok" {
		t.Fatalf("status = %q, want ok", body.Data.Status)
	}
}

func TestOpenAPI(t *testing.T) {
	srv := newTestServer(t, &stubProducer{})
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/openapi.json")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var body struct {
		OpenAPI string `json:"openapi"`
		Data    struct {
			Paths map[string]any `json:"paths"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if body.Data.Paths["/v1/send-message"] == nil {
		t.Fatal("missing /v1/send-message in spec")
	}
}

func TestMethodNotAllowed(t *testing.T) {
	srv := newTestServer(t, &stubProducer{})
	defer srv.Close()

	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/v1/send-message", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want 405", resp.StatusCode)
	}
}
