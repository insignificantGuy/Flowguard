// Command evidence serves the real HTTP/Kafka/consumer pipeline on isolated topics.
// It is a local experiment harness, not a production admin endpoint.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"flowguard/internal/consumer"
	"flowguard/internal/handler"
	"flowguard/internal/kafka"
	"flowguard/internal/logger"
	"flowguard/internal/observability"
	"flowguard/internal/resilience"
	"github.com/IBM/sarama"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/rs/zerolog"
)

type downstream struct {
	start                      atomic.Int64
	calls, failures, successes atomic.Int64
	scenario                   string
}

func (h *downstream) Handle(m *kafka.ConsumerMessage) error {
	h.calls.Add(1)
	delay := 50 * time.Millisecond
	if h.scenario == "breaker" {
		delay = 5 * time.Millisecond
	}
	time.Sleep(delay)
	elapsed := time.Since(time.Unix(0, h.start.Load()))
	if h.scenario == "breaker" && elapsed >= 2*time.Second && elapsed < 8*time.Second {
		h.failures.Add(1)
		return errors.New("simulated downstream outage: seconds 2 through 8")
	}
	h.successes.Add(1)
	return nil
}
func main() {
	broker := flag.String("broker", "localhost:39092", "Kafka bootstrap")
	topic := flag.String("topic", "", "fresh experiment topic (required)")
	mode := flag.String("mode", "enabled", "enabled or disabled")
	scenario := flag.String("scenario", "backpressure", "backpressure or breaker")
	flag.Parse()
	if *topic == "" || (*mode != "enabled" && *mode != "disabled") || (*scenario != "backpressure" && *scenario != "breaker") {
		log.Fatal("require topic, valid mode and scenario")
	}
	logger.Log = zerolog.New(os.Stderr).Level(zerolog.ErrorLevel)
	admin, err := sarama.NewClusterAdmin([]string{*broker}, sarama.NewConfig())
	must(err)
	// Fail on an existing topic: prior history must never enter the comparison.
	must(admin.CreateTopic(*topic, &sarama.TopicDetail{NumPartitions: 3, ReplicationFactor: 1}, false))
	must(admin.Close())
	metrics := observability.NewMetrics()
	h := &downstream{scenario: *scenario}
	c, err := consumer.NewConsumerWithMetrics(consumer.Config{
		Kafka:                   kafka.ConsumerConfig{Brokers: []string{*broker}, Topic: *topic, GroupID: *topic, ConsumerOffset: kafka.OffsetOldest},
		Backpressure:            resilience.BackpressureConfig{MaxQueueDepth: 512, PauseAfterDepth: 32, ResumeAfterDepth: 8, CheckInterval: time.Millisecond},
		CircuitBreakerThreshold: 3, CircuitBreakerCooldown: 2 * time.Second, MaxConcurrency: 4,
		DisableBackpressure: *mode == "disabled", DisableCircuitBreaker: *mode == "disabled",
	}, h, metrics)
	must(err)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		if err := c.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			log.Print(err)
		}
	}()
	producer, err := kafka.New(kafka.Config{Brokers: []string{*broker}, Timeout: 5 * time.Second})
	must(err)
	send := handler.NewSendMessage("Hello", *topic, producer, metrics)
	mux := http.NewServeMux()
	mux.Handle("/metrics", metrics.Handler())
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, "ok") })
	mux.HandleFunc("/v1/send-message", func(w http.ResponseWriter, r *http.Request) {
		h.start.CompareAndSwap(0, time.Now().UnixNano())
		send.ServeHTTP(w, r)
	})
	var peak, pausedSamples atomic.Int64
	go func() {
		t := time.NewTicker(5 * time.Millisecond)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				depth := c.BackpressureDepth()
				for old := peak.Load(); depth > old; old = peak.Load() {
					if peak.CompareAndSwap(old, depth) {
						break
					}
				}
				if c.BackpressureIsPaused() && *mode == "enabled" {
					pausedSamples.Add(1)
				}
			}
		}
	}()
	mux.HandleFunc("/report", func(w http.ResponseWriter, r *http.Request) {
		families, err := metricsRegistry(metrics)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		json.NewEncoder(w).Encode(map[string]interface{}{
			"mode": *mode, "scenario": *scenario, "topic": *topic, "time_utc": time.Now().UTC(),
			"downstream_calls": h.calls.Load(), "downstream_failures": h.failures.Load(), "succeeded": h.successes.Load(),
			"paused": c.BackpressureIsPaused(), "breaker_state": int(c.CircuitBreakerState()),
			"dead_lettered": c.DeadLetterQueue().Count(), "outstanding": c.BackpressureDepth(), "peak_outstanding": peak.Load(),
			"paused_samples_5ms": pausedSamples.Load(), "metrics": families,
		})
	})
	srv := &http.Server{Addr: "127.0.0.1:3330", Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	// Listen on all interfaces only for the local Docker Prometheus scraper.
	srv.Addr = ":3330"
	go func() {
		log.Printf("evidence ready topic=%s mode=%s scenario=%s", *topic, *mode, *scenario)
		if err := srv.ListenAndServe(); err != http.ErrServerClosed {
			must(err)
		}
	}()
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop
	shutdown, finish := context.WithTimeout(context.Background(), 10*time.Second)
	defer finish()
	must(srv.Shutdown(shutdown))
	must(c.Close())
	must(producer.Close())
}
func must(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

// Read the exact HTTP exposition used by Prometheus; no second registry.
func metricsRegistry(m *observability.Metrics) (map[string]float64, error) {
	result := map[string]float64{}
	// Counters here have a single topic; processing errors sum bounded reason labels.
	result["published"] = counterValue(m.MessagesPublishedTotal)
	result["consumed"] = counterValue(m.MessagesConsumedTotal)
	return result, nil
}

func counterValue(c prometheus.Collector) float64 {
	ch := make(chan prometheus.Metric)
	go func() { c.Collect(ch); close(ch) }()
	var sum float64
	for metric := range ch {
		v := &dto.Metric{}
		if err := metric.Write(v); err == nil {
			sum += v.GetCounter().GetValue()
		}
	}
	return sum
}
