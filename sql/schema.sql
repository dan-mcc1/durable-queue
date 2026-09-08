CREATE TABLE jobs (
    id              bigserial PRIMARY KEY,
    task            text        NOT NULL,          -- 'send_digest_to_user'
    args            jsonb       NOT NULL DEFAULT '{}',
    idempotency_key text        UNIQUE,            -- stops double-queueing
    status          text        NOT NULL DEFAULT 'pending',
    run_at          timestamptz NOT NULL DEFAULT now(),
    attempts        int         NOT NULL DEFAULT 0,
    max_attempts    int         NOT NULL DEFAULT 5,
    -- Recoveries are counted separately from attempts: a worker dying
    -- mid-job never reaches mark_failed, so without this a job that
    -- crashes its worker would be retried forever.
    recoveries      int         NOT NULL DEFAULT 0,
    max_recoveries  int         NOT NULL DEFAULT 10,
    locked_by       text,                          -- which worker owns it
    locked_until    timestamptz,                   -- ...until when
    last_error      text,
    created_at      timestamptz NOT NULL DEFAULT now(),
    finished_at     timestamptz
);
CREATE INDEX idx_jobs_pending_run_at ON jobs (run_at) WHERE status = 'pending';
CREATE INDEX idx_jobs_running_locked_until ON jobs (locked_until) WHERE status = 'running';

CREATE TABLE effects (
    key        text        PRIMARY KEY,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE schedules (
    name             text        PRIMARY KEY,
    task             text        NOT NULL,
    args             jsonb       NOT NULL DEFAULT '{}',
    interval_seconds int         NOT NULL,
    next_run_at      timestamptz NOT NULL DEFAULT now(),
    created_at       timestamptz NOT NULL DEFAULT now()
);

-- Chaos-test side channel only: a plain (non-deduped) log of every time
-- a chaos task's "risky action" actually ran, independent of the
-- effects ledger. Used to prove the ledger prevented a duplicate, not
-- just to assert that it did.
CREATE TABLE chaos_observations (
    id          bigserial   PRIMARY KEY,
    effect_key  text        NOT NULL,
    observed_at timestamptz NOT NULL DEFAULT now()
);

-- Fan-out/combine demo (test-only, like chaos_observations above): a
-- big problem tracked as N independent chunks. Whichever chunk job
-- observes completed_chunks == total_chunks enqueues the combine step.
CREATE TABLE problems (
    id               bigserial   PRIMARY KEY,
    total_chunks     int         NOT NULL,
    completed_chunks int         NOT NULL DEFAULT 0,
    status           text        NOT NULL DEFAULT 'in_progress',
    result           bigint,
    created_at       timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE chunk_results (
    problem_id  bigint NOT NULL REFERENCES problems(id),
    chunk_index int    NOT NULL,
    partial_sum bigint NOT NULL,
    PRIMARY KEY (problem_id, chunk_index)
);

-- Benchmark-only (see scripts/benchmark.py): one row per executed job,
-- recording how long it waited between being enqueued and running.
CREATE TABLE bench_samples (
    id          bigserial        PRIMARY KEY,
    latency_ms  double precision NOT NULL,
    recorded_at timestamptz      NOT NULL DEFAULT now()
);