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

All against local Postgres in Docker. `python scripts/benchmark.py [--suite comparison|curve|baseline|scaling|concurrency|durability|commitdelay|all]`

Numbers vary run to run by roughly ±15% at the faster configurations, where a
run drains in well under a second.

### Naive vs tuned

Both configurations measured back to back, same machine, same run, every commit
fsynced. "Naive" is one job claimed per round trip, run one at a time, woken by
polling — `--suite comparison`.

| | naive | tuned | |
|---|---|---|---|
| 1 worker process | 227 | 1649 | **7.3x** |
| 2 worker processes | 402 | 3019 | **7.5x** |
| 4 worker processes | 658 | 4446 | **6.8x** |
| 8 worker processes | 932 | **6078** | **6.5x** |
| enqueue → start (p50) | 719.0 ms | 6.9 ms | **104x** |

The tuned column is 8 slots per worker with a batch size of 25. The naive column
is already faster than the original code — reaper throttling, the reused
heartbeat connection, the shared heartbeat thread and the `(run_at, id)` index
are all unconditional now and can't be switched off — so this understates the
total gain rather than inflating it.

**Concurrency also pays for durability.** The ~50% durability cost is largely an
artifact of *low* concurrency. Every commit needs a WAL flush, but Postgres
group-commits concurrent transactions into shared flushes, so the more commits
in flight, the more of the fsync each one avoids paying for:

| | fully durable | workers relaxed | cost of durability |
|---|---|---|---|
| 4 in flight | 973 | 1925 | 49% |
| 16 in flight | 3065 | 4164 | 26% |
| 32 in flight | 4452 | 5626 | 21% |
| 64 in flight | 5882 | 6478 | **9%** |

So `durable_bookkeeping=False` matters far less than it first appeared — at real
concurrency you get full fsync-per-commit durability for single-digit percent,
and the default should stay on. Budget connections though: a process needs
`concurrency + 2`, so 8 workers × 8 slots is 80 against Postgres's default
`max_connections` of 100.

**How much of that is the library?** Same claim/work/complete cycle, single
process, with and without durable-queue in the path:

```
  raw Postgres, one at a time        160 jobs/sec
  durable-queue, batch of 1          179 jobs/sec   (+12% vs raw)
  durable-queue, batch of 10         281 jobs/sec   (+76% vs raw)
```

The library is now *faster* than hand-written one-at-a-time SQL, because
batching amortises a round trip the naive version pays per job.

### What actually determines throughput

Total concurrency — jobs in flight — and very little else. Not how you split it
between processes and threads (`--suite curve`):

| jobs in flight | split | jobs/sec | marginal gain |
|---|---|---|---|
| 1 | 1w × 1s | 262 | — |
| 2 | 1w × 2s | 510 | 1.95x |
| 4 | 1w × 4s | 905 | 1.77x |
| 8 | 1w × 8s | 1625 | 1.80x |
| 16 | 2w × 8s | 3048 | 1.88x |
| 32 | 4w × 8s | 4452 | 1.46x |
| 64 | 8w × 8s | 6098 | 1.37x |

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

- *Not WAL/fsync*, despite `WALWrite` waiters growing 1 → 3 → 7 → 12 with worker
  count. With `synchronous_commit = off` throughput doubles but the efficiency
  curve is unchanged (100/96/91/68% against 100/97/91/76%). Removing the
  dominant I/O wait didn't alter the shape, so it was a symptom, not the limit.
- *Not CPU or process count.* 16 cores; 8w×1s runs ~24 processes and 1w×8s ~11,
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

This benchmark also *understates* concurrency: `bench_job` waits on Postgres,
so slots contend on the same bottleneck. A task waiting on an HTTP call — what
ReleaseRadar's jobs actually do — overlaps far more cleanly.

An earlier version of this section reported processes beating threads roughly
2:1 and attributed it to the GIL. That was wrong: it was a barrier in the
worker loop (see the trail below) that penalised high slot counts specifically.
Kept here as a reminder that a plausible explanation for a real measurement can
still be the wrong one.

**What does durability cost, and which parts of it do you actually need?**

At 4 workers × 1 slot, where the fsync cost is most exposed:

```
  everything durable                 939 jobs/sec   (every commit fsynced)
  workers relaxed                   1977 jobs/sec   (enqueues still durable)
  nothing durable                   1977 jobs/sec   (an enqueue can vanish)
```

Not every commit needs to survive a crash. A worker makes two per job — the
claim and the completion — and losing either one is not data loss, it's a
re-run: the job reverts to `pending` and executes again, which is the
at-least-once path this queue already implements and tests. The commit that
genuinely must survive is the **enqueue**, because losing that means work was
requested and silently never happened — and that commit lives in the
application's transaction, not the worker's.

Since `synchronous_commit` is per-transaction, you can have both. Workers run
with `run_worker(durable_bookkeeping=False)`, enqueues stay fully durable, and
throughput roughly doubles — landing on top of the fully-relaxed number,
because worker commits vastly outnumber enqueue commits. Nothing the queue
guarantees is weakened.

The real cost is a higher chance of duplicate execution after a hard crash,
since completions confirmed just before it can disappear. Transactional tasks
are unaffected — their writes vanish with the completion and are simply redone.
Tasks with external side effects lean harder on their idempotency keys.

This is also the honest frame for comparing against a Redis-backed queue: much
of what such a system "wins" on throughput, it wins by not fsyncing. The
interesting question isn't who is faster, it's which commits each one is
willing to lose.

**What didn't work:** `commit_delay` (group commit) was expected to recover some
of that durability cost for free, since it batches fsyncs across concurrent
transactions without giving up any durability. Measured, it doesn't:

| commit_delay | 4 workers | 8 workers |
|---|---|---|
| 0µs | baseline | baseline |
| 500µs | +0% | +0% |
| 1000µs | −11% | +0% |
| 2000µs | −28% | −17% |

The reason, which only became clear later: Postgres was **already** group
committing. The durability table above shows the fsync cost falling from 49% to
9% purely by raising concurrency — that fall *is* group commit working on its
own. `commit_delay` exists to manufacture batching that isn't happening
naturally; here it already was, so the delay bought nothing and cost latency.
(An earlier version of this section blamed "not enough concurrent commits to
batch," which the durability measurements contradict.)

### The optimisation trail

Every number below is 4 worker processes, fully durable, measured by
`scripts/benchmark.py` — each step found by profiling rather than guessing:

| | jobs/sec |
|---|---|
| first measurement | 162 |
| reuse one heartbeat connection per worker (was one per *job*, ~13ms) | 386 |
| throttle the reaper (was sweeping before every job) | 489 |
| batch claiming + autocommit (claim was a round trip per job) | 844 |
| one heartbeat thread per worker (was one per job, ~0.13ms) | 865 |
| 8 concurrency slots per worker | 2400 |
| refill the slot queue continuously instead of draining each batch first | 3820 |
| index on `(run_at, id)`, bounded skip-locked reaper, one notify per batch | **4452** |

**27x at 4 workers**, all of it fully durable. At 8 workers × 8 slots the same
build reaches **6098 jobs/sec**.

### The stall that wasn't a slowness problem

Under load — 30k jobs, 8 workers × 8 slots — the queue would sometimes stop
dead. Not slow: **zero jobs processed**, indefinitely, until the workers were
killed. `pg_stat_activity` caught it: 63 backends waiting on
`LWLock:LockManager`, with granted `tuple` locks, meaning sessions were queued
behind each other's row locks — exactly what `SKIP LOCKED` is supposed to
prevent.

Three causes, found in order, each needing the previous one fixed to become
visible:

1. **The claim query sorted the entire pending backlog on every call.** A bulk
   enqueue gives every row an identical `run_at` (`now()` is fixed per
   transaction), and the index was on `run_at` alone — so `ORDER BY run_at, id`
   degenerated into one giant incremental-sort group. Claiming 25 jobs read and
   sorted all 30,000: **5.18ms and 2MB of sort memory per claim**, held while
   locking rows. Adding `id` to the index made it an index-ordered scan of
   exactly 25 rows: **0.052ms, ~100x**.
2. **The reaper was an unbounded `UPDATE ... WHERE status = 'running'` with no
   `SKIP LOCKED`**, so it blocked on any row a claim held — and every worker
   runs one. Now bounded and skip-locked; an expired lease missed this sweep is
   caught by the next.
3. **`enqueue` emits one notification per row.** Bulk-enqueueing 30,000 jobs
   fired 30,000 notifications, and on commit every listening worker woke in the
   same instant and issued its claim simultaneously. That synchronised stampede
   was the actual trigger — with NOTIFY disabled the stall vanished entirely
   (4/4 clean runs), which is what isolated it. `enqueue_many` inserts in one
   statement and notifies once.

After all three: **5/5 clean runs, ~6100 jobs/sec, and `LWLock:LockManager`
absent from the wait profile** — what remains is `WALWrite` and
`BufferContent`, which is just the cost of durable writes.

The lesson worth keeping: the first two fixes each improved throughput while
leaving the stall intact. A bug that only appears at scale can have several
independent causes stacked on top of one another, and fixing one just moves the
threshold.

### Fan-out: use `enqueue_many`

Calling `enqueue()` in a loop is wrong twice over — a round trip per job, and a
notification per job:

| 30,000 jobs | |
|---|---|
| `enqueue()` in a loop | ~30 s |
| `enqueue_many()` | **0.35 s** |

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
4. **A progress query that penalised the runs it measured.** `_drain` polled
   `count(*) WHERE status NOT IN (...)` — a seq scan costing 2.85ms, twenty
   times a second, growing with the table. Rephrased to match the partial index
   predicates it became two index-only scans at 0.086ms, and the 30k-job figure
   jumped 4286 → 5204.

The instrument distorted the result three separate times (twice above, plus a
`docker stats` sampler that starved the workers it was watching). Worth
remembering when reading any of these numbers.

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
durable-queue purge --older-than-hours 24   # drop completed jobs past retention
pytest
```

For fan-out, use `enqueue_many` rather than a loop of `enqueue` — see above.
For a production worker, `run_worker(concurrency=8, batch_size=25)` is the
measured sweet spot.

## Status

Milestones M1–M8 complete, plus transactional tasks, LISTEN/NOTIFY, batch
claiming, concurrency slots and the benchmark suite. **86 tests passing.**

Remaining: adoption in a real application (M9); the load-test/bloat write-up
(M10) — the bloat is now measured but `scripts/load_test.py` is still a stub;
and the production gaps that matter more than throughput — graceful shutdown
on SIGTERM, structured logging, reconnect after a dropped connection, real
migrations, and the `durable-queue worker` / `scheduler` commands DESIGN.md
specifies as the deploy entry points.
