-- Desired state. One row per service.
CREATE TABLE services (
    name                text PRIMARY KEY,
    image               text        NOT NULL,
    replicas            integer     NOT NULL CHECK (replicas BETWEEN 1 AND 50),
    generation          bigint      NOT NULL DEFAULT 1,
    observed_generation bigint      NOT NULL DEFAULT 0,
    status              text        NOT NULL DEFAULT 'Progressing',
    updated_at          timestamptz NOT NULL DEFAULT now()
);

-- One row per image change. At most one InProgress rollout per service.
CREATE TABLE rollouts (
    id            bigserial PRIMARY KEY,
    service       text        NOT NULL REFERENCES services(name) ON DELETE CASCADE,
    from_image    text        NOT NULL DEFAULT '',
    to_image      text        NOT NULL,
    state         text        NOT NULL DEFAULT 'InProgress',
    failed_checks integer     NOT NULL DEFAULT 0,
    created_at    timestamptz NOT NULL DEFAULT now(),
    finished_at   timestamptz
);
CREATE UNIQUE INDEX rollouts_one_active ON rollouts (service) WHERE state = 'InProgress';
CREATE INDEX rollouts_by_service ON rollouts (service, id DESC);

-- Running state, written by the simulated runtime.
CREATE TABLE instances (
    id         bigserial PRIMARY KEY,
    service    text        NOT NULL REFERENCES services(name) ON DELETE CASCADE,
    image      text        NOT NULL,
    healthy    boolean     NOT NULL DEFAULT false,
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX instances_by_service ON instances (service);

-- Work queue. Workers claim rows with FOR UPDATE SKIP LOCKED.
CREATE TABLE jobs (
    id           bigserial PRIMARY KEY,
    kind         text        NOT NULL,
    key          text        NOT NULL,
    run_at       timestamptz NOT NULL DEFAULT now(),
    attempts     integer     NOT NULL DEFAULT 0,
    max_attempts integer     NOT NULL DEFAULT 10,
    locked_by    text,
    locked_until timestamptz,
    requeue      boolean     NOT NULL DEFAULT false,
    last_error   text,
    done         boolean     NOT NULL DEFAULT false,
    dead         boolean     NOT NULL DEFAULT false,
    created_at   timestamptz NOT NULL DEFAULT now()
);
-- At most one pending job per (kind, key): enqueueing twice is a no-op.
CREATE UNIQUE INDEX jobs_one_pending ON jobs (kind, key) WHERE NOT done;
CREATE INDEX jobs_ready ON jobs (run_at) WHERE NOT done;
