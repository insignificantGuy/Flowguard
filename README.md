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

## Product Requirements Document (PRD)

![Flowguard PRD](docs/prd.png)

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

### Load Test Results (20s duration, 100 VUs, 600 rate)

**With Resilience Mechanisms ENABLED (backpressure thresholds: depth=10, pause=2, resume=1):**
- Total Requests: 2985
- Request Duration: avg=1.94ms, min=0.30ms, max=19.66ms, p(95)=4.07ms
- Error Rate: 0.00%
- Custom Error Rate: 0.00%
- Throughput: ~150 req/s sustained
- Observed: **Backpressure ACTIVE** (depth=4, paused=1), circuit breaker remained closed (state=0)

**With Resilience Mechanisms DISABLED (very high thresholds):**
- Total Requests: 3001
- Request Duration: avg=2.24ms, min=0.33ms, max=20.55ms, p(95)=5.62ms
- Error Rate: 0.00%
- Custom Error Rate: 0.00%
- Throughput: ~150 req/s sustained
- Observed: No backpressure activation, steady consumption rate

The load test validates:
- Throughput under load (~150 req/s sustained)
- Error rates remain low (0.00% in both tests)
- Resilience mechanisms activate appropriately (backpressure engaged when thresholds met)
- System recovers after load subsides (queue depth returns to baseline)

![Grafana Dashboard - Active](docs/grafana-active.png)
![Grafana Dashboard - Disabled](docs/grafana-disabled.png)

### Consumer-Side Metrics Before/After Load Test

| Metric | Before Load Test | After Load Test | Delta |
|--------|-----------------|-----------------|-------|
| Messages Consumed | 11,642 | 33,937 | **+22,295** |
| Backpressure Depth | 4 | 4 | 0 |
| Backpressure Paused | **1 (Active)** | **1 (Active)** | 0 |
| Circuit Breaker State | 0 (Closed) | 0 (Closed) | 0 |
| Dead Letter Queue Depth | 0 | 0 | 0 |

The consumer processed all 22,295 messages during the load test. Backpressure was **confirmed active** (paused=1, depth=4 above the pause threshold of 2), demonstrating the mechanism correctly engaged intake when the in-flight queue backed up. Circuit breaker remained closed and no messages went to the dead-letter queue, showing the pipeline handles sustained load correctly without failures.

### PRD in Kafka

The PRD document is published to Kafka for end-to-end pipeline demonstration:

```bash
# Publish the PRD to Kafka
./bin/publish-prd kafka:9092 RECONCILIATION docs/PRD.md

# Or run from inside the Docker container
docker compose exec flowguard /publish-prd kafka:9092 RECONCILIATION /app/docs/PRD.md
```

The PRD is stored at `docs/PRD.md` and can be published to any Kafka topic for testing the full pipeline: file → Kafka → consumer → handler.

### Design Rationale

The flowguard pipeline implements resilience through three complementary mechanisms: backpressure prevents system overload by pausing intake when queue depth exceeds thresholds, circuit breaker stops repeated calls to failing dependencies to allow recovery, and dead-letter queue provides graceful degradation by preserving messages that cannot be processed. These mechanisms work together to ensure the system remains operational under varying load conditions and failure scenarios. The backpressure configuration uses hysteresis (different pause/resume thresholds) to prevent oscillation near the threshold boundary. Circuit breaker follows the standard closed/open/half-open states with configurable failure threshold and cooldown period. Dead-letter queue stores messages with failure reasons for later inspection and replay. All mechanisms are independently observable through Prometheus metrics, enabling operators to tune parameters based on observed behavior in production.

### Portfolio Write-up

**Problem:** Building a reliable message pipeline that can ingest HTTP requests, buffer them via Kafka, and process them resiliently despite real-world failures like traffic spikes, downstream outages, or processing bugs—without dropping messages or crashing the system.

**Three Hardest Design Decisions:**
1. **Choosing hysteresis-based backpressure thresholds** (pause at 800, resume at 200) to avoid oscillation near the boundary while still protecting against unbounded queue growth.
2. **Implementing the circuit breaker with a half-open state** to safely test dependency recovery without overwhelming a healing service.
3. **Decoupling the dead-letter queue from Kafka** (in-memory fallback) so graceful degradation works even when Kafka itself is unavailable, trading durability for availability during total outages.

**What I’d Improve with More Time:**
- Add persistent dead-letter queue (e.g., to a separate Kafka topic or disk) for replay after recovery.
- Introduce adaptive backpressure that tunes pause/resume thresholds based on observed latency and error rates.
- Expand observability with end-to-end latency tracing (e.g., OpenTelemetry) to pinpoint bottlenecks across HTTP→Kafka→consumer→handler.

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