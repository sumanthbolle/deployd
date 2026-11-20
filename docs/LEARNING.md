# Learning guide

Read the code in this order. Each step says what to look for, why it was built that way, and a question you should be able to answer afterwards (in an interview, too). Exercises at the end extend the system.

## 0. Set up

```sh
brew install go postgresql@17
brew services start postgresql@17
createdb deployd_test
export DATABASE_URL=postgres://localhost:5432/deployd_test?sslmode=disable
make test-db
```

Go basics you will meet: packages and `internal/`, structs and methods, interfaces (satisfied implicitly), errors as values (`errors.Is`, `errors.As`, `%w`), `context.Context`, goroutines, `sync.WaitGroup`, `select`. The [Go tour](https://go.dev/tour) covers all of them in an evening.

## 1. The contract: `proto/deployd/v1/deployd.proto`

The API is defined once in protobuf. `buf generate` produces Go types (`gen/deployd/v1`) and a Connect handler/client (`gen/.../deploydv1connect`). Connect serves three protocols from one handler: gRPC (HTTP/2, binary), gRPC-Web, and Connect (plain HTTP POST with JSON, which is why `curl` works).

Why `generation` and `observed_generation`: the client changes desired state instantly, but the system catches up later. Comparing the two numbers tells you whether the running state reflects the latest spec. Kubernetes uses the same idea.

**Answer:** what breaks for existing clients if you renumber a protobuf field?

## 2. Entry point: `cmd/deployd/main.go`

Follow `run()` top to bottom: config from the environment, logger, signal handling (`signal.NotifyContext` turns SIGTERM into a cancelled context), database pool, migrations, two HTTP servers, the worker pool, then graceful shutdown.

Look at the two listeners. The API port can require mTLS; probes and metrics sit on a separate plain port because the kubelet and Prometheus don't present client certificates.

**Answer:** in what order does shutdown happen, and why do workers get a fresh context for an in-flight pass?

## 3. Storage: `internal/store/`

`migrate.go` embeds SQL files into the binary (`//go:embed`) and applies them inside a PostgreSQL advisory lock, so two replicas booting together don't both run a migration.

`store.go`: notice the `Querier` interface. `*pgxpool.Pool` and `pgx.Tx` both satisfy it, so the same function works inside or outside a transaction. `UpdateService` locks the row (`FOR UPDATE`), bumps `generation`, and opens a rollout. When a new image arrives during a rollout, it keeps the old rollout's `from_image`: the target it was moving to was never proven healthy, so it is not a safe place to roll back to.

**Answer:** why is `rollouts_one_active` a *partial* unique index?

## 4. The work queue: `internal/queue/queue.go` (the most important file)

- `Claim` uses `FOR UPDATE SKIP LOCKED`: concurrent workers skip rows another transaction holds, so no two take the same job and none wait on each other.
- Each claim sets `locked_until` (a lease). A crashed worker's job becomes claimable again when the lease runs out. `TestExpiredLeaseIsReclaimed` shows it.
- `jobs_one_pending` allows one pending job per service, so a burst of updates becomes one reconcile.
- The `requeue` flag handles a classic race: a change lands while its job is running. Without the flag, `Complete` would mark the job done and the change would be skipped. `TestEnqueueWhileRunningRequeues` covers it.
- Failures back off exponentially (`Backoff`) and become `dead` after `max_attempts`, kept for inspection rather than retried forever.

The API enqueues in the *same transaction* that saves the spec (see `api/service.go`). This is the transactional-outbox idea: the change and the work to apply it can't get out of sync.

**Answer:** what happens if a pass takes longer than the lease? (Then read the `FOR UPDATE` in `reconciler.go`.)

## 5. Reconciliation: `internal/reconcile/`

`plan.go` is a pure function: (desired, observed) → actions. No database, no clock, so it is easy to test exhaustively. `TestRollingUpdateNeverDropsCapacity` simulates a whole rollout and checks that healthy capacity never falls below the replica count.

`reconciler.go` runs one pass in one transaction: lock the service row, probe health, check the rollout health gate, plan, act. It is idempotent: a pass with nothing to do changes nothing. After `FailureThreshold` unhealthy passes it rolls back by pointing the desired image at the last good one; the next passes converge on it like any other change.

`worker.go`: N goroutines claim jobs. `Progressing` means come back shortly (`Retry`), `Done` completes the job, and an error counts as a failure with backoff. Polling uses jitter so replicas don't hit the database in lockstep.

**Answer:** why put the rollback decision in the reconciler and not in the API?

## 6. API, auth and errors: `internal/api/`

- `AuthInterceptor` checks every RPC: no token gives `Unauthenticated`, a viewer calling a write gives `PermissionDenied`. Token comparison is constant time.
- `toConnectErr` maps storage errors to API codes and never leaks internal error text to clients.
- `validate` rejects bad input before it reaches the database. The database has its own `CHECK` constraint as a second line of defence.

**Answer:** what is the difference between `Unauthenticated` and `PermissionDenied`, and which HTTP status does each map to?

## 7. Observability: `internal/telemetry/`

Logs are JSON with `service` and `generation` fields, so you can filter one rollout's story. Metrics cover reconcile outcomes and durations, finished rollouts by state, queue depth and API calls by code. Traces are exported over OTLP when `OTEL_EXPORTER_OTLP_ENDPOINT` is set.

Try it: deploy a broken image and watch `deployd_rollouts_finished_total{state="RolledBack"}` go up.

## 8. Shipping: `Dockerfile`, `deploy/helm/`, `.github/workflows/ci.yml`

- Multi-stage build to a distroless, non-root image with a static binary.
- Helm: `maxUnavailable: 0` rolling updates, a PodDisruptionBudget, liveness and readiness probes kept separate, a read-only root filesystem, dropped capabilities, and optional mTLS from a Secret.
- CI runs the race detector against a real PostgreSQL service container, lints the protobuf and the chart, and publishes the image to GHCR from `main`.

Try it locally with kind: `kind create cluster`, build and load the image, create the `deployd-secrets` Secret, then `helm install deployd deploy/helm/deployd`.

## Exercises (in rising difficulty)

1. Add a `DeleteService` RPC (deployer only). Instances should be stopped by the reconciler, not by the API.
2. Add `max_unavailable` to the planner so large services can update faster. Extend the table test first.
3. Add a canary step: start one new instance, hold for N passes, then continue. Store the step on the rollout.
4. Add a gRPC server-streaming RPC `WatchService` that pushes status changes (hint: PostgreSQL `LISTEN/NOTIFY`).
5. Write a `runtime.Kubernetes` that implements `Runtime` with client-go, creating one Pod per instance.
6. Add a scheduled job that re-enqueues every service every few minutes so drift (an instance dying outside deployd) gets repaired.
7. Add OpenTelemetry metrics alongside Prometheus, then run Jaeger in docker-compose and look at a trace of one rollout.
