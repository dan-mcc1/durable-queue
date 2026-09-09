# durable-queue

A standalone Python library that turns a Postgres table into a durable job runner. Your app calls `enqueue(...)`; separate worker processes claim jobs, run them, and survive being killed.

No Redis, no Kafka, no broker — Postgres only. That's the thesis, not a limitation: because the queue is a table in the same database, enqueueing a job and committing your business data happen in **one transaction**. A queue in Redis cannot give you that.

```python
with conn.transaction():
    cur.execute("INSERT INTO watchlist_entries (...) VALUES (...)")
    enqueue(conn, "notify_friends", {"user_id": user_id})
# both land, or neither does
```

See [DESIGN.md](DESIGN.md) for the full scope, architecture, data model, and milestone ladder.

## Exactly-once, for effects that live in Postgres

Exactly-once delivery is impossible in general — if your side effect is an
HTTP call, no amount of bookkeeping closes the window between doing it and
recording that you did. But if the effect is a database write, it can share
the job's transaction, and then it genuinely is exactly-once.

A task that declares a `conn` parameter gets that automatically:

```python
@task
def award_credit(conn, user_id: str, amount: int):
    with conn.cursor() as cur:
        cur.execute("UPDATE accounts SET credit = credit + %s WHERE id = %s", (amount, user_id))
    # no commit, no idempotency key, no ledger - the worker commits this
    # write and the job's completion together, or neither
```

Two chaos tests make the difference concrete. Both kill real worker
subprocesses mid-job, at random, with no cleanup opportunity:

| | duplicated effects |
|---|---|
| effects-ledger task (external-shaped side effect) | 4–6 out of 24 |
| transactional task (`conn` parameter) | **0 out of 24** |

## What's built

| | |
|---|---|
| **Atomic claiming** | `FOR UPDATE SKIP LOCKED`, so N workers never hand out the same job twice |
| **Crash recovery** | Leases + heartbeats + a reaper, with a ceiling so a hung task can't hold its lease forever |
| **Retries** | Exponential backoff with full jitter; poison pills that crash their worker are dead-lettered by recovery count |
| **Idempotency** | Enqueue-time dedup via `idempotency_key`, effect-time dedup via an effects ledger, and true atomicity for transactional tasks |
| **Low latency** | `LISTEN/NOTIFY` wakes an idle worker on enqueue, with polling retained as the backstop for retries and schedules |
| **Scheduling** | Recurring schedules with `pg_try_advisory_lock` leader election, plus a per-run idempotency backstop |
| **Observability** | `durable-queue ls / show / retry / dead / stats` |
| **Chaos tests** | Real worker subprocesses killed mid-job, asserting invariants across many jobs and many kills |

## Benchmarks

All against local Postgres in Docker. `python scripts/benchmark.py [--suite baseline|scaling|durability|all]`

**Throughput and latency** (4 workers):

```
  throughput                   386 jobs/sec

  enqueue -> start       LISTEN/NOTIFY      polling only
  p50                        9.5 ms           717.7 ms
  p99                       13.0 ms           813.1 ms
```

**How much of that is the library?** Same claim/work/complete cycle, single
process, with and without durable-queue in the path:

```
  raw Postgres claim/complete        158 jobs/sec
  durable-queue                      122 jobs/sec
  library overhead                    23%
```

**Does `SKIP LOCKED` actually scale?** Worker count vs. throughput:

| workers | jobs/sec | vs 1 worker | efficiency |
|---|---|---|---|
| 1 | 121 | 1.00x | 100% |
| 2 | 221 | 1.82x | 91% |
| 4 | 386 | 3.19x | 80% |
| 8 | 700 | 5.78x | 72% |

**What does durability cost?** The same run with fsync-per-commit relaxed to
roughly where a Redis-backed queue sits by default:

```
  synchronous_commit = on            386 jobs/sec   (every commit fsynced)
  synchronous_commit = off           711 jobs/sec   (commits may be lost on crash)
  cost of durability                  46%
```

That last number is the honest frame for any comparison against a Redis-backed
queue: roughly half this queue's throughput is spent being durable. A system
that doesn't fsync will win the benchmark, and that's what it's buying with the
win.

The harness earned its keep twice. Its first run showed 162 jobs/sec and a 35ms
p50 — profiling the loop found `process_one` opening a fresh heartbeat
connection per job (~13ms locally, over half the per-job budget), and reusing
one per worker took it to 386/sec and 9.5ms. Its second finding was a bug in
the harness itself: a `SELECT` that never committed held a read lock on `jobs`
and deadlocked the next phase's `TRUNCATE`.

## Running it

```bash
docker compose up -d
docker compose exec -T postgres psql -U durable_queue -d durable_queue_dev < sql/schema.sql
pip install -e ".[cli,test]"

python -m durable_queue.worker      # a worker
python -m durable_queue.scheduler   # the scheduler
durable-queue stats                 # inspect the queue
pytest
```

## Status

Milestones M1–M8 complete, 59 tests passing. Remaining: adoption in a real
application (M9) and the load-test/bloat investigation (M10).
