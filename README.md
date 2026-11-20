# deployd

A small deployment control plane in Go. You declare what should run (an image and a replica count); deployd drives the running instances toward it with rolling updates, and rolls back automatically when a new image fails its health checks.

It is built the way production control planes are: a gRPC and HTTP API in front, a durable PostgreSQL work queue in the middle, and idempotent reconcile workers behind it that any number of replicas can run safely.

```
            gRPC / Connect / REST                 FOR UPDATE SKIP LOCKED
 client ──────────────────────────► API ──┐      ┌──────────────────────► worker 1 ─┐
        bearer token (RBAC), mTLS         │ tx   │                        worker 2 ─┤ reconcile:
                                          ▼      │                        worker N ─┤ probe → plan → act
                                 ┌──────────────────────┐                           │
                                 │ PostgreSQL           │◄──────────────────────────┘
                                 │  services  (desired) │
                                 │  instances (actual)  │
                                 │  rollouts  (history) │
                                 │  jobs      (queue)   │
                                 └──────────────────────┘
```

## What it shows

| Area | Where |
|---|---|
| gRPC, gRPC-Web and HTTP/JSON from one protobuf contract (Connect) | `proto/`, `internal/api/service.go` |
| Read-only REST endpoints | `internal/api/http.go` |
| Bearer-token RBAC (viewer / deployer), mutual TLS | `internal/api/auth.go`, `cmd/deployd/main.go` |
| PostgreSQL schema, embedded migrations under an advisory lock | `internal/store/` |
| Durable work queue: `SKIP LOCKED`, leases, backoff, dead jobs, no lost updates | `internal/queue/` |
| State reconciliation: desired vs actual, pure planner, idempotent passes | `internal/reconcile/` |
| Progressive delivery: rolling update with surge, health gate, automatic rollback | `internal/reconcile/reconciler.go` |
| Observability: JSON logs, Prometheus metrics, OpenTelemetry traces | `internal/telemetry/` |
| Graceful shutdown on SIGTERM, liveness vs readiness probes | `cmd/deployd/main.go` |
| Docker (distroless, non-root), Helm chart with PDB and ServiceMonitor | `Dockerfile`, `deploy/helm/` |
| CI: gofmt, vet, race-enabled tests on real PostgreSQL, buf lint, helm lint, image | `.github/workflows/ci.yml` |

The runtime (where instances "run") is simulated in a PostgreSQL table so everything works on a laptop. An image whose tag contains `broken` never becomes healthy, which lets you watch a rollback happen. A real backend (Kubernetes, VMs, bare metal) would implement the three-method `runtime.Runtime` interface.

## Run it

With Docker:

```sh
docker compose up --build        # PostgreSQL + two deployd replicas
```

Without Docker (needs Go 1.26+ and PostgreSQL):

```sh
createdb deployd
export DATABASE_URL=postgres://localhost:5432/deployd?sslmode=disable
make run
```

## Try it

```sh
D='Authorization: Bearer dev-deployer'
V='Authorization: Bearer dev-viewer'
J='Content-Type: application/json'

# Create a service with three replicas.
curl -s -H "$D" -H "$J" localhost:8080/deployd.v1.DeployService/CreateService \
  -d '{"name":"web","image":"web:1","replicas":3}'

# Watch it become Ready (REST).
curl -s -H "$V" localhost:8080/v1/services/web

# Rolling update to a good image, then to a broken one.
curl -s -H "$D" -H "$J" localhost:8080/deployd.v1.DeployService/UpdateService -d '{"name":"web","image":"web:2"}'
# ...wait until web:2 is Ready, then:
curl -s -H "$D" -H "$J" localhost:8080/deployd.v1.DeployService/UpdateService -d '{"name":"web","image":"web:3-broken"}'

# The broken rollout is RolledBack and the service runs web:2 again.
curl -s -H "$V" -H "$J" localhost:8080/deployd.v1.DeployService/ListRollouts -d '{"name":"web"}'

# A viewer cannot deploy: permission_denied.
curl -s -H "$V" -H "$J" localhost:8080/deployd.v1.DeployService/UpdateService -d '{"name":"web","replicas":5}'

# Health and metrics live on the internal ops port.
curl -s localhost:9090/readyz
curl -s localhost:9090/metrics | grep deployd_
```

gRPC clients (for example `grpcurl` or the generated Go client) use the same port over HTTP/2.

## Test

```sh
make test                                     # unit tests
DATABASE_URL=postgres://... make test-db      # plus PostgreSQL integration tests
```

The database tests create a throwaway schema per test, so they run in parallel against one database.

## Configuration

| Variable | Default | Meaning |
|---|---|---|
| `DATABASE_URL` | required | PostgreSQL connection string |
| `DEPLOYD_TOKENS` | required | `token:viewer,token:deployer` |
| `DEPLOYD_ADDR` / `DEPLOYD_OPS_ADDR` | `:8080` / `:9090` | API and ops (health, metrics) listeners |
| `DEPLOYD_TLS_CERT`, `DEPLOYD_TLS_KEY` | unset | serve the API over TLS |
| `DEPLOYD_CLIENT_CA` | unset | require client certificates signed by this CA (mTLS) |
| `DEPLOYD_WORKERS` | `4` | reconcile workers per replica |
| `DEPLOYD_MAX_SURGE` | `1` | extra instances allowed during a rollout |
| `DEPLOYD_FAILURE_THRESHOLD` | `3` | unhealthy passes before rollback |
| `DEPLOYD_JOB_LEASE` | `30s` | how long a claimed job is reserved |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | unset | send traces to an OTLP collector |

## Learning guide

[docs/LEARNING.md](docs/LEARNING.md) walks through the code in reading order, with the design decisions and exercises.

## License

MIT
