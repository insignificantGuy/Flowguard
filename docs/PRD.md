# Flowguard Product Requirements Document (PRD)

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
- Comparison runs with resilience mechanisms enabled/disabled

### Phase 4: Ship
- System runnable end-to-end with one command (`docker-compose up`)
- README fully written: problem statement, architecture diagram, resilience mechanisms explained with design decisions, how to run it, load-test results embedded
- No placeholder or "(planned)" language anywhere
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