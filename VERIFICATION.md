# Verification — 24 Oct 2026

The earlier uncontrolled October runs and their enabled/disabled claims are
superseded by the paired experiments linked in [README.md](README.md).

- [Raw paired results and exact settings](docs/evidence/results.json)
- [Breaker transition log](docs/evidence/breaker-enabled.log)
- [Race-enabled local test result](docs/evidence/tests.txt)
- [Function coverage](docs/evidence/coverage.txt)
- [Reproduction runner](scripts/run-evidence.py)

Run:

```bash
go test ./... -race -count=1 -coverprofile=coverage.out -covermode=atomic
go vet ./...
docker compose -p flowthrough-evidence -f deploy/evidence/compose.yml up -d --wait
go build -o bin/evidence ./cmd/evidence
python3 scripts/run-evidence.py
```

The runner fails if k6 fails or drops iterations, a topic already exists,
messages do not drain within the deadline, or dashboard capture fails. Each run
starts with a fresh topic and group and zero counters. It samples every 100ms;
the server tracks peak outstanding count every 5ms. There are no forced breaker
state changes; the injected downstream outage opens it and a successful probe
closes it. Screenshots show an explicit historical time within each experiment,
captured after load generation to avoid adding browser CPU load to the test.

These are local demonstration results, not a production capacity claim. Kafka
redelivery after crashes remains possible; the fallback DLQ is process-local.
