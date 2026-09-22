# durable-queue

A standalone Python library that turns a Postgres table into a durable job runner. Your app calls `enqueue(...)`; separate worker processes claim jobs, run them, and survive being killed.

No Redis, no Kafka, no broker — Postgres only. That's the thesis, not a limitation: because the queue is a table in the same database, enqueueing a job and committing your business data happen in **one transaction**. A queue in Redis cannot give you that.

```python
with conn.transaction():
    cur.execute("INSERT INTO watchlist_entries (...) VALUES (...)")
    enqueue(conn, "notify_friends", {"user_id": user_id})
# both land, or neither does
```

See [DESIGN.md](DESIGN.md) for the architecture and data model.

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

|                                                   | duplicated effects |
| ------------------------------------------------- | ------------------ |
| effects-ledger task (external-shaped side effect) | 4–6 out of 24      |
| transactional task (`conn` parameter)             | **0 out of 24**    |

## Features

|                     |                                                                                                                               |
| ------------------- | ----------------------------------------------------------------------------------------------------------------------------- |
| **Atomic claiming** | `FOR UPDATE SKIP LOCKED`, so N workers never hand out the same job twice                                                      |
| **Crash recovery**  | Leases + heartbeats + a reaper, with a ceiling so a hung task can't hold its lease forever                                    |
| **Retries**         | Exponential backoff with full jitter; poison pills that crash their worker are dead-lettered by recovery count                |
| **Idempotency**     | Enqueue-time dedup via `idempotency_key`, effect-time dedup via an effects ledger, and true atomicity for transactional tasks |
| **Low latency**     | `LISTEN/NOTIFY` wakes an idle worker on enqueue, with polling retained as the backstop for retries and schedules              |
| **Scheduling**      | Recurring schedules with `pg_try_advisory_lock` leader election, plus a per-run idempotency backstop                          |
| **Observability**   | `durable-queue ls / show / retry / dead / stats`                                                                              |
| **Chaos tests**     | Real worker subprocesses killed mid-job, asserting invariants across many jobs and many kills                                 |

## Benchmarks

All against local Postgres in Docker. `python scripts/benchmark.py [--suite comparison|curve|baseline|scaling|concurrency|durability|commitdelay|all]`

Numbers vary run to run by roughly ±15% at the faster configurations, where a
run drains in well under a second.

### Naive vs tuned

Both configurations measured back to back, same machine, same run, every commit
fsynced. "Naive" is one job claimed per round trip, run one at a time, woken by
polling — `--suite comparison`. "Tuned" is 8 slots per worker with a batch size
of 25.

|                       | naive    | tuned    |          |
| --------------------- | -------- | -------- | -------- |
| 1 worker process      | 227      | 1649     | **7.3x** |
| 2 worker processes    | 402      | 3019     | **7.5x** |
| 4 worker processes    | 658      | 4446     | **6.8x** |
| 8 worker processes    | 932      | **6078** | **6.5x** |
| enqueue → start (p50) | 719.0 ms | 6.9 ms   | **104x** |

**How much of that is the library?** Same claim/work/complete cycle, single
process, with and without durable-queue in the path:

```
  raw Postgres, one at a time        160 jobs/sec
  durable-queue, batch of 1          179 jobs/sec   (+12% vs raw)
  durable-queue, batch of 10         281 jobs/sec   (+76% vs raw)
```

The library is _faster_ than hand-written one-at-a-time SQL, because batching
amortises a round trip the naive version pays per job.

### What actually determines throughput

Total concurrency — jobs in flight — and very little else. Not how you split it
between processes and threads (`--suite curve`):

| jobs in flight | split   | jobs/sec | marginal gain |
| -------------- | ------- | -------- | ------------- |
| 1              | 1w × 1s | 262      | —             |
| 2              | 1w × 2s | 510      | 1.95x         |
| 4              | 1w × 4s | 905      | 1.77x         |
| 8              | 1w × 8s | 1625     | 1.80x         |
| 16             | 2w × 8s | 3048     | 1.88x         |
| 32             | 4w × 8s | 4452     | 1.46x         |
| 64             | 8w × 8s | 6098     | 1.37x         |

**Read scaling numbers against this curve, not in isolation.** At a fixed
concurrency of 8, how you split it barely matters — 8w×1s gives 1570, 4w×2s
1571, 2w×4s 1561, 1w×8s 1625. So "8 workers gave me 6.5x but 8 slots only gave
me 3.5x" is not two different scaling behaviours; it's the same curve walked
over different ranges. A naive worker running one job at a time goes from 1 to 8
jobs in flight — the near-linear left end. A tuned worker running 8 slots goes
from 8 to 64 — the flattening right end. The tell: naive at 8 processes (932
jobs/sec) and tuned at 1 process (1649) are the same order, because both are 8
jobs in flight.

Scaling efficiency measures how much unused capacity you started with, not how
good the configuration is.

**Why the knee?** Two plausible causes, both ruled out by measurement:

- _Not WAL/fsync_, despite `WALWrite` waiters growing 1 → 3 → 7 → 12 with worker
  count. With `synchronous_commit = off` throughput doubles but the efficiency
  curve is unchanged (100/96/91/68% against 100/97/91/76%). Removing the
  dominant I/O wait didn't alter the shape, so it was a symptom, not the limit.
- _Not CPU or process count._ 16 cores; 8w×1s runs ~24 processes and 1w×8s ~11,
  and both land at ~1600 jobs/sec.

What remains is contention on the shared hot pages of a single queue — every
worker scans the same leftmost region of the pending index and dirties the same
heap pages, and `BufferContent` appears in the wait profile at high concurrency.
That's inherent to one-table-one-queue: consumers necessarily compete for the
head. Sharding (`WHERE id % N = shard`) would relieve it at the cost of global
FIFO ordering. This one is inferred from the wait profile rather than proven the
way the other two were disproven.

**Slots are the cheaper way to buy concurrency.** A process costs
`concurrency + 2` connections, so 8w×1s needs 24 Postgres connections where
1w×8s needs 10 for the same throughput. Threads rather than async, because task
functions are ordinary blocking Python and the GIL is released while they wait
on IO.

**4 workers × 8 slots is the sensible operating point** at ~4450 jobs/sec: going
to 8×8 buys +37% while doubling connections from 40 to 80, against a default
`max_connections` of 100.

This benchmark also _understates_ concurrency: `bench_job` waits on Postgres,
so slots contend on the same bottleneck. A task waiting on an HTTP call
overlaps far more cleanly.

### Durability

**Concurrency pays for durability.** Every commit needs a WAL flush, but
Postgres group-commits concurrent transactions into shared flushes, so the more
commits in flight, the more of the fsync each one avoids paying for:

|              | fully durable | workers relaxed | cost of durability |
| ------------ | ------------- | --------------- | ------------------ |
| 4 in flight  | 973           | 1925            | 49%                |
| 16 in flight | 3065          | 4164            | 26%                |
| 32 in flight | 4452          | 5626            | 21%                |
| 64 in flight | 5882          | 6478            | **9%**             |

So `durable_bookkeeping=False` matters far less than it first appears — at real
concurrency you get full fsync-per-commit durability for single-digit percent,
and the default should stay on.

`commit_delay` (group commit) buys nothing here and costs latency (−11% at
1000µs, −28% at 2000µs with 4 workers), because Postgres is _already_ group
committing — that is exactly what the 49% → 9% fall above is.

**Which commits actually need to survive a crash?** A worker makes two per job —
the claim and the completion — and losing either is not data loss, it's a
re-run: the job reverts to `pending` and executes again, which is the
at-least-once path this queue already implements and tests. The commit that
genuinely must survive is the **enqueue**, because losing that means work was
requested and silently never happened — and that commit lives in the
application's transaction, not the worker's.

Since `synchronous_commit` is per-transaction, you can have both. Workers run
with `run_worker(durable_bookkeeping=False)`, enqueues stay fully durable, and
throughput roughly doubles, because worker commits vastly outnumber enqueue
commits. Nothing the queue guarantees is weakened. The real cost is a higher
chance of duplicate execution after a hard crash, since completions confirmed
just before it can disappear. Transactional tasks are unaffected — their writes
vanish with the completion and are simply redone. Tasks with external side
effects lean harder on their idempotency keys.

This is also the honest frame for comparing against a Redis-backed queue: much
of what such a system "wins" on throughput, it wins by not fsyncing. The
interesting question isn't who is faster, it's which commits each one is
willing to lose.

### Why the claim path looks the way it does

Under load — 30k jobs, 8 workers × 8 slots — the queue would stop dead: zero
jobs processed, indefinitely, with 63 backends waiting on `LWLock:LockManager`
holding granted `tuple` locks, which is exactly what `SKIP LOCKED` is supposed
to prevent. Three independent causes, each now a constraint worth keeping:

1. **The index is on `(run_at, id)`, not `run_at` alone.** A bulk enqueue gives
   every row an identical `run_at` (`now()` is fixed per transaction), so
   `ORDER BY run_at, id` degenerated into one giant incremental-sort group:
   claiming 25 jobs read and sorted all 30,000 at **5.18ms and 2MB of sort
   memory per claim**, held while locking rows. The composite index makes it an
   index-ordered scan of exactly 25 rows: **0.052ms**.
2. **The reaper is bounded and skip-locked.** As an unbounded
   `UPDATE ... WHERE status = 'running'` it blocked on any row a claim held —
   and every worker runs one. An expired lease missed by one sweep is caught by
   the next.
3. **`enqueue_many` inserts in one statement and notifies once.** Per-row
   notifications meant 30,000 jobs woke every listening worker 30,000 times,
   and on commit they all issued claims in the same instant. That synchronised
   stampede was the actual trigger — with NOTIFY disabled the stall vanished
   entirely.

After all three: 5/5 clean runs at ~6100 jobs/sec, with `LWLock:LockManager`
absent from the wait profile — what remains is `WALWrite` and `BufferContent`,
the cost of durable writes.

### Fan-out: use `enqueue_many`

Calling `enqueue()` in a loop is wrong twice over — a round trip per job, and a
notification per job:

| 30,000 jobs           |            |
| --------------------- | ---------- |
| `enqueue()` in a loop | ~30 s      |
| `enqueue_many()`      | **0.35 s** |

### Reading these numbers

This is Docker Desktop on Windows, where fsync goes through a virtualised
filesystem and is unusually slow. The relative measurements — overhead, scaling
efficiency, durability cost — are the ones worth quoting. The 8-worker runs
drain in under a second at these job counts, so those rows are the least
precise. Measurement itself distorted the results three separate times here (a
per-job heartbeat connection, a seq-scan progress query, and a `docker stats`
sampler that starved the workers it was watching), which is worth remembering
when reading any benchmark.

## Running it

```bash
docker compose up -d
docker compose exec -T postgres psql -U durable_queue -d durable_queue_dev < sql/schema.sql
pip install -e ".[cli,test]"

python -m durable_queue.worker      # a worker
python -m durable_queue.scheduler   # the scheduler
durable-queue stats                 # inspect the queue
durable-queue purge --older-than-hours 24   # drop completed jobs past retention
durable-queue purge --older-than-hours 24   # drop completed jobs past retention
pytest
```

For fan-out, use `enqueue_many` rather than a loop of `enqueue` — see above.
For a production worker, `run_worker(concurrency=8, batch_size=25)` is the
measured sweet spot.

For a production worker, `run_worker(concurrency=8, batch_size=25)` is the
measured sweet spot. Budget connections: a process needs `concurrency + 2`, so
8 workers × 8 slots is 80 against Postgres's default `max_connections` of 100.

## Known gaps

No graceful shutdown on SIGTERM, no structured logging, no reconnect after a
dropped connection, no real migrations, and no `durable-queue worker` /
`scheduler` CLI entry points. 86 tests passing.
