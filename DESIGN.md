# durable-queue — design doc

(Working name during design: "keel". Repo/package name settled on `durable-queue` / `durable_queue`.)

## 1. Scope & shape

A standalone Python library, its own repo, that turns a Postgres table into a
durable job runner. An application calls `enqueue(...)`; separate worker
processes claim jobs, run them, and survive being killed. [ReleaseRadar](../ReleaseRadar)
is the first real adopter, migrating three of its background loops onto it
at the end of the ladder (see M9).

### Non-goals (deliberate)

These matter more than the feature list — "I chose not to build X, here's
why" is itself the interview answer.

- **No Redis, no Kafka, no broker. Postgres only.** Not a limitation — a
  thesis: you already have a transactional database, so enqueueing a job and
  committing your business data can happen in one transaction. Redis can't
  give you that. This is the single best design decision in the project and
  should be defensible cold.
- **No multi-machine cluster.** Multiple worker processes on one box. All the
  interesting concurrency lives in table contention, and four local
  processes fully exercise it.
- **No web dashboard.** CLI only. React is already proven elsewhere; a
  dashboard adds no new signal here.
- **No priority queues, rate limiting, or DAG dependencies.** Linear
  workflow steps only.
- **No multi-tenancy or auth.** It's a library, not a service.

### Key design choice: one job per worker process

Each worker process runs one job at a time; you scale by starting more
processes. Simpler lease handling, simpler shutdown, and contention between
processes still exercises everything that matters.

**Honest tradeoff:** the target workloads are IO-bound (TMDb calls, email
sends), so one-at-a-time leaves throughput on the table — a real queue would
run N concurrent slots per worker. Correctness comes first; concurrency
slots are a later, clean incremental addition (see stretch, M11+) rather
than a thing that complicates every earlier milestone.

### Deliverables

The library repo, a CLI, a chaos test suite, a README with before/after
failure demos, and 2–3 of ReleaseRadar's eight background loops actually
migrated onto it.

---

## 2. Architecture & data model

### The picture

Four processes, one database. Nothing talks to anything else directly —
every process only talks to Postgres. No network protocol to design, no
service discovery, no message format. The database is the entire
coordination mechanism.

```
┌─────────────────┐
│  FastAPI app    │──enqueue()──┐
└─────────────────┘             │
                                 ▼
┌─────────────────┐      ┌──────────────────────┐        ┌──────────────┐
│   Scheduler     │─────▶│  Postgres           | ◀────▶│  Worker × N  │
│ "anything due?" │      │  durable_queue jobs  │        │ claim→run→done│
└─────────────────┘      └──────────────────────┘        └──────────────┘
                                 ▲
                          ┌──────┴──────┐
                          │     CLI     │  "what ran? what failed?"
                          └──────┴──────┘
```

### Tables

**`jobs`** — the to-do list. One row per piece of work.

```sql
CREATE TABLE jobs (
    id              bigserial PRIMARY KEY,
    task            text        NOT NULL,          -- 'send_digest_to_user'
    args            jsonb       NOT NULL DEFAULT '{}',
    idempotency_key text        UNIQUE,             -- stops double-queueing
    status          text        NOT NULL DEFAULT 'pending',
    run_at          timestamptz NOT NULL DEFAULT now(),
    attempts        int         NOT NULL DEFAULT 0,
    max_attempts    int         NOT NULL DEFAULT 5,
    locked_by       text,                           -- which worker owns it
    locked_until    timestamptz,                     -- ...until when
    last_error      text,
    created_at      timestamptz NOT NULL DEFAULT now(),
    finished_at     timestamptz
);
```

Only four states: `pending → running → succeeded`, or `→ dead` after too
many failures. A job that fails but has retries left goes back to `pending`
with a future `run_at`. Fewer states, less to get wrong.

- **`schedules`** — the recurring stuff. One row per "every hour" / "3am
  daily" entry. This is where ReleaseRadar's eight loops end up.
- **`effects`** — the "I already did this" ledger. One row per irreversible
  thing that happened (see below).
- **`workflow_steps`** — remembers which steps of a multi-step job already
  finished. Last milestone only (M11, stretch).

### Two clever bits

**1. Enqueue happens inside your transaction.** Because the queue is a table
in the same database:

```python
with db.begin():
    db.add(new_watchlist_entry)
    enqueue(db, "notify_friends", {"user_id": uid})
```

Either both land or neither does. You can't queue a notification for a
watchlist entry that failed to save. If the queue were Redis, this guarantee
is simply unavailable. This is the argument for the whole design — lead with
it when asked "why not Redis?"

**2. The duplicate-effect problem, and why there's no clean answer.**

Worker sends the digest email → crashes before it can mark the row done →
lease expires → another worker picks it up → second email.

The unique key on `jobs` does not save you here — that only stops the same
job being _queued_ twice. This is one job _running_ twice. Different
problem.

Recording the effect first doesn't fix it either, it just moves the crash
window:

- Record first, then send → crash in between means the email is never sent,
  but the ledger claims it was. (At-most-once.)
- Send first, then record → crash in between means the email sends twice.
  (At-least-once.)

There is no third option — you cannot wrap an HTTP call to a mail provider
in a Postgres transaction. Every queue system faces this and picks a side.

**The answer:** pick at-least-once (never lose an email), then make the
duplicate harmless by passing the provider a stable idempotency key (e.g.
`digest:2026-09-07:user_abc`). The provider absorbs the repeat. Duplicates
become impossible at the boundary you don't control, not the one you do.

### Deployment reality

This adds one process type. On Render/Railway/Fly that's a second small
service running `durable-queue worker`. Roughly one more cheap instance.
Running the worker as a subprocess inside the API container avoids that
cost but re-couples the two things being separated — pay the few dollars.

---

## 3. Milestone ladder

Ten milestones over four weeks (honest arithmetic: ~25 evenings, closer to
five weeks). Each milestone teaches exactly one concept and leaves a working
system — stopping early still means stopping with something real.

### Week 1 — a queue that works

- **M1 · The table** (2 evenings) — a queue is just a table; work must exist
  as a row before anyone attempts it. Build: schema, `@task` registry,
  `enqueue()`, one worker that polls → claims → runs → marks done, claimed
  naively on purpose (`SELECT … WHERE status='pending' LIMIT 1`, then
  `UPDATE`).
- **M2 · Safe claiming** (2 evenings) — M1 has a race: two workers `SELECT`
  the same row before either `UPDATE`s it. Failure mode: run four workers,
  the same job runs four times. Build: atomic claim via
  `UPDATE … WHERE id = (SELECT … FOR UPDATE SKIP LOCKED ORDER BY run_at, id LIMIT 1) RETURNING *`,
  plus a partial index `ON jobs (run_at) WHERE status = 'pending'`. Know why
  `SKIP LOCKED` beats plain `FOR UPDATE` (skips contended rows vs. queuing
  behind them and serializing workers).
- **M3 · Crash recovery** ⭐ (3 evenings) — kill a worker mid-job and the row
  sits in `running` forever; nobody retries it. Build: `locked_until`, a
  heartbeat that extends the lease while working, a reaper that returns
  expired leases to `pending`. `locked_by` should encode enough identity
  (host + pid + a short random suffix) that the reaper can't confuse two
  workers. The core insight: you cannot distinguish a crashed worker from a
  slow one. A lease is a bet — too short double-runs healthy jobs, too long
  makes recovery crawl. This is where at-least-once delivery stops being a
  phrase and becomes a consequence you derived.

### Week 2 — a queue you can trust

- **M4 · Retries** (2 evenings) — transient failures should retry, permanent
  ones shouldn't retry forever. Failure mode: a failing task hot-loops, or
  500 failing jobs all retry at the same instant and flatten the database.
  Build: `attempts` / `max_attempts`, `run_at = now() + backoff(attempts) + jitter`,
  `dead` status when exhausted. Know what a thundering herd is and why
  jitter — not backoff alone — prevents it.
- **M5 · Idempotency** ⭐ (3 evenings) — the hard one. At-least-once means a
  job can run twice; if it sends email, that's two emails. Build:
  `idempotency_key` on `jobs` for enqueue-time dedupe
  (`ON CONFLICT DO NOTHING`), plus the separate `effects` ledger for
  effect-time dedupe. Verify whether the mail provider accepts an
  idempotency key on send — that determines the final design. These are two
  different problems that most people conflate; exactly-once delivery is
  impossible; for any external side effect you must choose at-most-once or
  at-least-once, and the right move is at-least-once plus a stable key
  pushed to the provider.
- **M6 · Scheduling & leader election** (2 evenings) — recurring work, and
  the "two API instances, two emails" bug that started this project. Build:
  `schedules` table, scheduler loop, `pg_try_advisory_lock()` for
  leadership, with `sched:{name}:{run_at}` as an idempotency backstop
  (bucket `run_at` deterministically — e.g. truncate to the hour — before
  embedding it in the key, or a scheduler restart can compute a slightly
  different timestamp and silently lose the dedupe guarantee). Know why two
  independent defenses are used instead of one, and that a session-scoped
  advisory lock releasing automatically when the connection dies is the
  feature, not a limitation.

### Week 3 — a queue you can operate

- **M7 · Observability** (2 evenings) — build:
  `durable-queue ls | show <id> | retry <id> | dead | stats`. Queue depth
  alone is a misleading metric; oldest-pending-job age is what you actually
  alert on — depth of 10,000 draining in a second is fine, depth of 3 stuck
  for an hour is an outage.
- **M8 · Chaos testing** ⭐ (3 evenings) — stop asserting guarantees, prove
  them. Build: a harness that enqueues N jobs with recorded effects, runs M
  workers, `SIGKILL`s them at randomized points (not `SIGTERM` — the point
  is proving recovery from zero cleanup opportunity, the actual failure mode
  M3 defends against), then checks invariants: every job reached a terminal
  state, no effect key appears twice, nothing was lost. This tests
  properties, not examples — the single artifact that separates this from a
  tutorial.

### Week 4 — the payoff

- **M9 · Adopt it in ReleaseRadar** (3 evenings) — migrate three loops: the
  digest (fan out to one job per user — the best story), the episode
  refresh (TMDb flakes, so retries earn their keep), and Stripe
  reconciliation (where idempotency matters most). It's in production in
  something real, plus a note on what the API got wrong that only real use
  revealed.
- **M10 · Load test → find the bloat → fix it** ⭐⭐ (3 evenings) — the
  signature Postgres-as-a-queue failure. Every `UPDATE`/`DELETE` leaves dead
  tuples; under sustained load autovacuum falls behind, the index bloats,
  the claim query degrades badly. Build: sustained load, measure claim
  latency over time, watch it fall over. Diagnose with `n_dead_tup` in
  `pg_stat_user_tables` and `pgstattuple`. Fix by archiving completed rows
  out, partitioning, or per-table autovacuum tuning. Re-measure and publish
  both curves. This is the exact production problem Oban had to ship
  partitioning for — hit it, diagnosed it, fixed it.

### Stretch — cut without guilt

- **M11 · Durable workflows** (3+ evenings) — `workflow_steps` memoization
  so a multi-step job resumes at step 3 instead of restarting. Explicitly
  optional; M1–M10 is a complete, defensible project.

If running long: cut M11 first, then trim M7 to a single stats command. Do
not cut M8 or M10 — they're the two that make this more than a tutorial.
