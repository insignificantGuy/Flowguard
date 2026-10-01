# syntax=docker/dockerfile:1

# ---------------------------------------------------------------------------
# Builder
# ---------------------------------------------------------------------------
FROM golang:1.26-alpine AS builder

WORKDIR /src

RUN addgroup -g 1001 -S appgroup && adduser -u 1001 -S appuser -G appgroup

ENV CGO_ENABLED=0 GOOS=linux GOARCH=amd64

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN go build -ldflags="-s -w -X main.version=1.0.0 -X main.commit=unknown -X main.date=2026-01-01T00:00:00Z" \
    -trimpath -o /out/flowguard ./cmd/server

# ---------------------------------------------------------------------------
# Runtime — minimal distroless or alpine
# ---------------------------------------------------------------------------
FROM alpine:3.21 AS runtime

ENV ENV=production PORT=3000 HOST=0.0.0.0 LOG_LEVEL=info

COPY --from=builder /out/flowguard /flowguard

# Create user at runtime (alpine may not have same uid/gid)
RUN addgroup -g 1001 -S appgroup && adduser -u 1001 -S appuser -G appgroup
USER appuser

EXPOSE 3000

HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD ["sh", "-c", "wget -qO- http://127.0.0.1:3000/health | grep -q ok || exit 1"]

CMD ["/flowguard"]