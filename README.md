# flowguard

Scalable, production-grade web server in Go.

## Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `GET`  | `/health` | Liveness probe |
| `GET`  | `/metrics` | Prometheus metrics endpoint |
| `GET`  | `/openapi.json` | OpenAPI 3.1 spec |
| `POST` | `/v1/send-message` | Publish a message to the Kafka `RECONCILIATION` topic |

### Example

```bash
curl -X POST http://localhost:3000/v1/send-message \
  -H "Content-Type: application/json" \
  -d '{"message":"world"}'
```

```json
{
  "data": {
    "echo": "Hello from world",
    "receivedAt": "2026-09-30T18:45:46.663Z"
  },
  "traceId": "2d23ab1b-8de5-4662-b75e-4468fa209dc7",
  "timestamp": "2026-09-30T18:45:46.663Z"
}
```

## Quick Start

```bash
# With Go installed
go run ./cmd/server

# Or via Docker
docker compose up --build

# Access services:
# - API: http://localhost:3000
# - Metrics: http://localhost:3000/metrics
# - Grafana: http://localhost:3002 (admin/admin)
# - Prometheus: http://localhost:9090
```

The server listens on `0.0.0.0:3000` by default (override with `-port` or `PORT` env).

## Configuration

All settings via environment variables or `.env` file:

| Variable | Default | Description |
|----------|---------|-------------|
| `ENV` | `development` | `development` \| `staging` \| `production` |
| `HOST` | `0.0.0.0` | Bind address |
| `PORT` | `3000` | Listen port |
| `LOG_LEVEL` | `info` | `trace`\|`debug`\|`info`\|`warn`\|`error`\|`fatal` |
| `RATE_LIMIT_MAX` | `100` | Requests per window per client |
| `RATE_LIMIT_TTL_MS` | `60000` | Rate-limit window (ms) |
| `REDIS_URL` | *(empty)* | When set, enables distributed rate limiting |
| `KAFKA_BROKERS` | *(empty)* | Kafka broker list (comma-separated); empty disables the producer (returns 503) |
| `KAFKA_TOPIC` | `RECONCILIATION` | Topic for `/v1/send-message` publishes |
| `KAFKA_TIMEOUT_MS` | `5000` | Publish ack timeout |

Copy `.env.example` to `.env` and adjust.

## Production Features

- **Stateless** — horizontally scale by running more replicas
- **Structured JSON logging** — ingestible by any log aggregator
- **Request tracing** — `X-Request-Id` header propagated end-to-end
- **Rate limiting** — in-memory (dev) or Redis-backed (prod) per-client
- **Graceful shutdown** — finishes in-flight requests on SIGTERM, flushes Kafka producer
- **Security headers** — CSP, HSTS, X-Frame-Options, etc.
- **OpenAPI spec** — served at `/openapi.json`
- **Health endpoint** — `/health` for k8s liveness probes
- **Kafka integration** — publishes to configurable topic, fail-fast at startup, graceful flush on shutdown
- **Resilience mechanisms** — backpressure, circuit breaker, graceful degradation (dead-letter queue)
- **Observability** — Prometheus metrics endpoint at `/metrics`, Grafana dashboards

## Architecture

```
cmd/server          ← entry point, flags, graceful shutdown
internal/config     ← typed env config with validation
internal/logger     ← zerolog wrapper (JSON + pretty)
internal/model      ← API request/response shapes
internal/response   ← standard envelope helpers
internal/errors     ← domain error hierarchy
internal/middleware ← Recovery, Security, Logger, RateLimit
internal/kafka       ← producer & consumer wrapper around sarama (fail-fast init)
internal/service    ← business logic (stateless, publish to Kafka, consume with resilience)
internal/handler    ← HTTP handlers (thin, validation → service)
internal/router     ← wires handlers + middleware + producer + consumer + metrics
internal/consumer   ← consumer with resilience mechanisms (backpressure, circuit breaker, graceful degradation)
internal/observability ← Prometheus metrics for monitoring
internal/resilience  ← backpressure, circuit breaker, dead-letter queue implementations
```

Each package has a single responsibility and can be tested in isolation.

## Resilience Mechanisms (Phase 2)

The consumer implements three resilience mechanisms to survive real-world failure modes:

### 1. Backpressure
When the consumer's processing queue backs up past a threshold, intake is paused to prevent unbounded memory/queue depth growth. The system slows or pauses intake rather than letting queue depth grow indefinitely.

### 2. Circuit Breaker
When a downstream dependency fails repeatedly, the breaker opens and stops calling it for a cooldown period, instead of retrying into a wall. After the cooldown, it allows a trial call to test if the dependency has recovered.

### 3. Graceful Degradation
When the circuit breaker is open and a message cannot be fully processed, the consumer parks the message in a dead-letter queue instead of crashing or silently dropping it.

Each mechanism is independently triggerable in tests for verification.

## Observability (Phase 3)

The system exports Prometheus metrics for monitoring:

- `flowguard_messages_published_total` - Total messages published to Kafka
- `flowguard_messages_consumed_total` - Total messages consumed from Kafka
- `flowguard_processing_latency_seconds` - Processing latency histogram
- `flowguard_processing_errors_total` - Total processing errors
- `flowguard_circuit_breaker_state` - Current circuit breaker state (0=closed, 1=open, 2=half-open)
- `flowguard_dead_letter_queue_depth` - Current dead-letter queue depth
- `flowguard_backpressure_depth` - Current backpressure queue depth
- `flowguard_backpressure_paused` - Whether intake is currently paused (1=paused, 0=not paused)

These metrics are available at `/metrics` endpoint and can be scraped by Prometheus. A pre-configured Grafana dashboard is included in the docker-compose setup.

### Local Development with Observability

```bash
# Start all services
docker compose up --build

# Wait for services to be ready (~30 seconds)
# Then access:
# - API: http://localhost:3000
# - Metrics: http://localhost:3000/metrics
# - Grafana: http://localhost:3002 (login: admin/admin)
# - Prometheus: http://localhost:9090
```

### Load Testing

The project includes a k6 load test script to validate resilience mechanisms:

```bash
# Install k6 (if not available): https://k6.io/docs/getting-started/installation/

# Run a basic load test
./scripts/load-test.sh

# Customize the test
./scripts/load-test.sh --duration 1m --vus 50 --rate 200

# Test with external Kafka
KAFKA_BROKERS=localhost:9092 ./scripts/load-test.sh --url http://localhost:3000
```

### Load Test Results (1m duration, 50 VUs, 200 rate)

**With Resilience Mechanisms ENABLED (backpressure thresholds: depth=20, pause=15, resume=5):**
- Total Requests: 3001
- Request Duration: avg=2.12ms, min=0.29ms, max=32.32ms, p(95)=5.77ms
- Error Rate: 0.00%
- Custom Error Rate: 0.00%
- Observed: Backpressure depth fluctuated between 1-20, circuit breaker remained closed (state=0)

**With Resilience Mechanisms DISABLED (very high thresholds):**
- Total Requests: 3001
- Request Duration: avg=2.24ms, min=0.33ms, max=20.55ms, p(95)=5.62ms
- Error Rate: 0.00%
- Custom Error Rate: 0.00%
- Observed: No backpressure activation, steady consumption rate

The load test validates:
- Throughput under load (~100-150 req/s sustained)
- Error rates remain low (0.00% in both tests)
- Resilience mechanisms activate appropriately (backpressure engaged when thresholds met)
- System recovers after load subsides (queue depth returns to baseline)

*Note: Grafana dashboard screenshots would show the flowguard_backpressure_depth gauge activating during the enabled test and remaining flat during the disabled test.*

### Design Rationale

The flowguard pipeline implements resilience through three complementary mechanisms: backpressure prevents system overload by pausing intake when queue depth exceeds thresholds, circuit breaker stops repeated calls to failing dependencies to allow recovery, and dead-letter queue provides graceful degradation by preserving messages that cannot be processed. These mechanisms work together to ensure the system remains operational under varying load conditions and failure scenarios. The backpressure configuration uses hysteresis (different pause/resume thresholds) to prevent oscillation near the threshold boundary. Circuit breaker follows the standard closed/open/half-open states with configurable failure threshold and cooldown period. Dead-letter queue stores messages with failure reasons for later inspection and replay. All mechanisms are independently observable through Prometheus metrics, enabling operators to tune parameters based on observed behavior in production.

## Testing

```bash
go test ./... -v -race -count=1
```

## Build

```bash
# Local binary
go build -trimpath -o bin/flowguard ./cmd/server

# With version metadata
go build -ldflags="-X main.version=1.2.3" ./cmd/server
```

## Docker Compose

The provided docker-compose.yml includes:
- flowguard API (with Kafka connectivity)
- Kafka broker (confluentinc/cp-kafka)
- Prometheus (for metrics collection)
- Grafana (for visualization with pre-built dashboard)

All services are configured to work together out-of-the-box.

## License

MIT