# flowguard

Scalable, production-grade web server in Go.

## Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `GET`  | `/health` | Liveness probe |
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
docker build -t flowguard .
docker run -p 3000:3000 flowguard
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

## Architecture

```
cmd/server          ← entry point, flags, graceful shutdown
internal/config     ← typed env config with validation
internal/logger     ← zerolog wrapper (JSON + pretty)
internal/model      ← API request/response shapes
internal/response   ← standard envelope helpers
internal/errors     ← domain error hierarchy
internal/middleware ← Recovery, Security, Logger, RateLimit
internal/kafka       ← producer wrapper around sarama (fail-fast init)
internal/service    ← business logic (stateless, publish to Kafka)
internal/handler    ← HTTP handlers (thin, validation → service)
internal/router     ← wires handlers + middleware + producer
```

Each package has a single responsibility and can be tested in isolation.

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

## License

MIT