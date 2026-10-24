# Flowguard Product Requirements Document (PRD)

## Broker decision — 24 Oct 2026

**Kafka is the broker for this repository.** This repository-local PRD supersedes
the RabbitMQ wording in the original Notion planning page. It does not require
publishing the PRD itself as a message. The external Notion page has not been edited.

Use Kafka topics/partitions, consumer groups and completion offsets rather than
exchanges, queue bindings or RabbitMQ ack/nack terminology. Completion marks are
committed only through contiguous finished records in each partition. Kafka can
redeliver after a crash; this is at-least-once delivery, not exactly once. A
process-local DLQ is the current demonstration fallback and is not durable.

## Overview

Flowguard is a scalable, production-grade web server in Go that provides a
message pipeline with built-in resilience mechanisms. It ingests messages
via HTTP, publishes them to Kafka, and consumes them with backpressure,
circuit breaker, and dead-letter queue protection.

## Requirements

### Phase 1: Core Pipeline
- HTTP API endpoint for sending messages
- Kafka producer with fail-fast initialization
- Graceful shutdown with producer flush
- Standard response envelope with trace IDs
- Rate limiting (in-memory or Redis-backed)
- OpenAPI 3.1 spec
- Health endpoint for liveness probes

### Phase 2: Resilience Mechanisms
- **Backpressure**: Pause intake when queue depth exceeds thresholds
- **Circuit Breaker**: Stop calling failing dependencies for a cooldown period
- **Graceful Degradation**: Park unprocessable messages in a dead-letter queue

### Phase 3: Observability & Load Testing
- Prometheus metrics endpoint
- Grafana dashboard for visualization
- k6 load testing script
- Like-for-like enabled/disabled comparisons using fresh topics and groups
- Overload run must show paused intake in Grafana and a bounded outstanding count
- Failure run must record open → half-open → closed transitions and downstream call reduction
- After drain: published = consumed = succeeded + dead-lettered; no historical counters
- Store raw k6 summaries, consumer snapshots, logs and Grafana captures with results

### Phase 4: Ship
- System runnable end-to-end with one command (`docker-compose up`)
- README fully written: problem statement, architecture diagram, resilience mechanisms explained with design decisions, how to run it, load-test results embedded
- Documentation must state remaining limitations without claiming unverified acceptance criteria
- Portfolio write-up paragraph written (problem, three hardest design decisions, what would be improved with more time)
- Roadmap checklist (all 4 phases) fully checked

## Architecture

```
HTTP → Kafka → Consumer → Handler
                ↓
     Backpressure / Circuit Breaker / Dead Letter Queue
```

## Key Metrics

- `flowguard_messages_published_total` - Total messages published to Kafka
- `flowguard_messages_consumed_total` - Total messages consumed from Kafka
- `flowguard_processing_latency_seconds` - Processing latency histogram
- `flowguard_processing_errors_total` - Total processing errors
- `flowguard_circuit_breaker_state` - Current circuit breaker state
- `flowguard_dead_letter_queue_depth` - Current dead-letter queue depth
- `flowguard_backpressure_depth` - Current backpressure queue depth
- `flowguard_backpressure_paused` - Whether intake is currently paused

## Configuration

All settings via environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `ENV` | `development` | deployment environment |
| `PORT` | `3000` | HTTP listen port |
| `KAFKA_BROKERS` | *(empty)* | Kafka broker list |
| `KAFKA_TOPIC` | `RECONCILIATION` | Topic for messages |
| `BACKPRESSURE_MAX_QUEUE_DEPTH` | `1000` | Max queue depth |
| `BACKPRESSURE_PAUSE_AFTER_DEPTH` | `800` | Pause threshold |
| `BACKPRESSURE_RESUME_AFTER_DEPTH` | `200` | Resume threshold |
| `CIRCUIT_BREAKER_THRESHOLD` | `3` | Failure threshold |
| `CIRCUIT_BREAKER_COOLDOWN_MS` | `30000` | Cooldown period |