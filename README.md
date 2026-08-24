# TyphoonRelief G10

TyphoonRelief is a Go backend for coordinating typhoon warnings, evacuation orders, shelters, rescue dispatch, supply movements, public alerts and an auditable incident closeout.

## Run locally

```bash
GOTOOLCHAIN=local go test ./... -count=1
GOTOOLCHAIN=local go run ./cmd/server
```

The server exposes `/healthz` and `/readyz`. It uses a local SQLite database and versioned migrations; no online service is required for tests.

## Operations

`make test`, `make race`, `make vet`, `make build`, and `make docker-build` are the supported checks. The process handles SIGINT/SIGTERM, drains the worker, and closes the database after HTTP shutdown.
