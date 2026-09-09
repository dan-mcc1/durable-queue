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

All against local Postgres in Docker. `python scripts/benchmark.py [--suite baseline|scaling|durability|commitdelay|all]`

**Throughput and latency** (4 workers):

```
  throughput                   489 jobs/sec

  enqueue -> start       LISTEN/NOTIFY      polling only
  p50                        8.0 ms           717.1 ms
  p99                        9.3 ms           810.2 ms
```

**How much of that is the library?** Same claim/work/complete cycle, single
process, with and without durable-queue in the path:

```
  raw Postgres claim/complete        156 jobs/sec
  durable-queue                      154 jobs/sec
  library overhead                     1%
```

**Does `SKIP LOCKED` actually scale?** Worker count vs. throughput:

| workers | jobs/sec | vs 1 worker | efficiency |
|---|---|---|---|
| 1 | 148 | 1.00x | 100% |
| 2 | 276 | 1.86x | 93% |
| 4 | 487 | 3.28x | 82% |
| 8 | 800 | 5.39x | 67% |

**What does durability cost?** The same run with fsync-per-commit relaxed to
roughly where a Redis-backed queue sits by default:

```
  synchronous_commit = on            487 jobs/sec   (every commit fsynced)
  synchronous_commit = off          1017 jobs/sec   (commits may be lost on crash)
  cost of durability                  52%
```

That last number is the honest frame for any comparison against a Redis-backed
queue: about half this queue's throughput is spent being durable. A system that
doesn't fsync will win the benchmark, and that's what it's buying with the win.

**What didn't work:** `commit_delay` (group commit) was expected to recover some
of that durability cost for free, since it batches fsyncs across concurrent
transactions without giving up any durability. Measured, it doesn't:

| commit_delay | 4 workers | 8 workers |
|---|---|---|
| 0µs | baseline | baseline |
| 500µs | +0% | +0% |
| 1000µs | −11% | +0% |
| 2000µs | −28% | −17% |

The reason it can't help is the same one that makes the queue fast: each worker
runs claim → work → complete serially, so there are rarely enough simultaneous
commits in flight for group commit to batch. The workload is bound by
round-trip latency, not fsync bandwidth, and `commit_delay` trades exactly the
former for the latter.

### What the harness found

Three times the benchmark paid for itself:

1. **162 → 386 jobs/sec.** The first run was slower than it should have been;
   profiling found `process_one` opening a fresh heartbeat connection per job
   (~13ms locally, over half the per-job budget). Reused one per worker.
2. **23% → 1% overhead.** The remaining gap against raw Postgres was almost
   entirely the reaper sweep running before *every job* — a commit and two
   `UPDATE`s for work that only matters once per lease period. Throttling it to
   a quarter of the lease closed the gap and lifted every configuration 14–27%.
3. **A bug in the harness itself** — a `SELECT` that never committed held a read
   lock on `jobs` and deadlocked the next phase's `TRUNCATE`. Each phase passed
   alone; only the sequence hung.

One caveat on the absolute figures: this is Docker Desktop on Windows, where
fsync goes through a virtualised filesystem and is unusually slow. The relative
measurements — overhead, scaling efficiency, durability cost — are the ones
worth quoting. The 8-worker runs also drain in under a second at these job
counts, so that row is the least precise.

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
