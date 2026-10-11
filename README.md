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
| **Retries**         | Exponential backoff with full jitter; `PermanentError` dead-letters at once; per-task `@task(max_attempts=...)`; poison pills that crash their worker are dead-lettered by recovery count |
| **Idempotency**     | Enqueue-time dedup via `idempotency_key`, effect-time dedup via an effects ledger, and true atomicity for transactional tasks |
| **Low latency**     | `LISTEN/NOTIFY` wakes an idle worker on enqueue, with polling retained as the backstop for retries and schedules              |
| **Scheduling**      | Intervals or clock times in a time zone; missed runs collapse to one. Leaderless: due schedules are locked `FOR UPDATE SKIP LOCKED`, so any number of schedulers can run, plus a per-run idempotency backstop |
| **Connections**     | Slots borrow from a per-process pool only for database work, so steady-state connections follow database work, not slot count; dropped connections are reopened on next use |
| **Observability**   | `durable-queue ls / show / retry / dead / stats`                                                                              |
| **Chaos tests**     | Real worker subprocesses killed mid-job, asserting invariants across many jobs and many kills                                 |

## Benchmarks

All against local Postgres in Docker. Every table below comes from one
command, with the suite named beside it:

```bash
python scripts/benchmark.py --suite <name> --jobs 4000 --workers 4 --repeat 3
```

Each throughput figure is the median of three runs; see
[Reading these numbers](#reading-these-numbers) for how runs are taken and why.

### Naive vs tuned

Both configurations measured back to back, same machine, same run, every commit
fsynced. "Naive" is one job claimed per round trip, run one at a time, woken by
polling — `--suite comparison`. "Tuned" is 8 slots per worker with a batch size
of 25.

|                       | naive    | tuned    |          |
| --------------------- | -------- | -------- | -------- |
| 1 worker process      | 237      | 1579     | **6.7x** |
| 2 worker processes    | 439      | 2899     | **6.6x** |
| 4 worker processes    | 774      | 4148     | **5.4x** |
| 8 worker processes    | 1377     | **5455** | **4.0x** |
| enqueue → start (p50) | 750.8 ms | 6.7 ms   | **112x** |

The speedup shrinks with more processes because naive has more room to grow:
it starts at the steep left end of the curve below. An earlier version of this
table showed 6.5x at 8 processes. Its runs were short enough that a naive
worker's wait for its first poll, up to a second, was likely a large share of
each one.

**How much of that is the library?** Same claim/work/complete cycle, single
process, with and without durable-queue in the path:

```
  raw Postgres, one at a time        154 jobs/sec
  durable-queue, batch of 1          173 jobs/sec   (+12% vs raw)
  durable-queue, batch of 10         270 jobs/sec   (+75% vs raw)
```

(`--suite baseline`, a single in-process run rather than a median.)

The library is _faster_ than hand-written one-at-a-time SQL, because batching
amortises a round trip the naive version pays per job.

### What actually determines throughput

Total concurrency — jobs in flight — and very little else. Not how you split it
between processes and threads (`--suite curve`):

| jobs in flight | split   | jobs/sec | marginal gain |
| -------------- | ------- | -------- | ------------- |
| 1              | 1w × 1s | 254      | —             |
| 2              | 1w × 2s | 495      | 1.95x         |
| 4              | 1w × 4s | 869      | 1.75x         |
| 8              | 1w × 8s | 1563     | 1.80x         |
| 16             | 2w × 8s | 2987     | 1.91x         |
| 32             | 4w × 8s | 4057     | 1.36x         |
| 64             | 8w × 8s | 5617     | 1.38x         |

**Read scaling numbers against this curve, not in isolation.** At a fixed
concurrency of 8, how you split it barely matters — 8w×1s gives 1596, 4w×2s
1610, 2w×4s 1588, 1w×8s 1609. So "8 naive workers gave me 5.8x but 8 tuned
workers only gave me 3.5x" is not two different scaling behaviours; it's the
same curve walked over different ranges. A naive worker running one job at a
time goes from 1 to 8 jobs in flight — the near-linear left end. A tuned worker
running 8 slots goes from 8 to 64 — the flattening right end. The tell: naive
at 8 processes (1377 jobs/sec) and tuned at 1 process (1579) are the same
order, because both are 8 jobs in flight.

Scaling efficiency measures how much unused capacity you started with, not how
good the configuration is.

**Why the knee?** Two plausible causes, both ruled out by measurement. The
figures in this part come from that original investigation, not the
tables above:

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

**Slots are the cheaper way to buy concurrency.** A process costs up to
`concurrency + 2` connections, and `bench_job`, being transactional, uses all of
them, so 8w×1s needs 24 Postgres connections where 1w×8s needs 10 for the same
throughput. Tasks that aren't transactional need far fewer; see
[Connections](#connections). Threads rather than async, because task functions
are ordinary blocking Python and the GIL is released while they wait on IO.

**4 workers × 8 slots is the sensible operating point** at ~4,100 jobs/sec
(4057–4148 across these suites): going to 8×8 buys about +38% while doubling
connections from 40 to 80, against a default `max_connections` of 100.

This benchmark also _understates_ concurrency: `bench_job` waits on Postgres,
so slots contend on the same bottleneck. A task waiting on an HTTP call
overlaps far more cleanly.

### Connections

Slots used to own a connection each, and a task waiting 20ms on an HTTP call
held it idle the whole time. Now they borrow from a per-process pool for each
piece of database work: the whole run for a transactional task, which needs
it throughout, and only the completion write for anything else. `--suite
connections`, with `io_job` standing in for that HTTP call:

| `io_job` (20ms, no `conn`) | peak connections, owned | pooled | jobs/sec, owned | pooled   |
| -------------------------- | ----------------------- | ------ | --------------- | -------- |
| 1w × 8s                    | 11                      | 7      | 258             | 257      |
| 1w × 32s                   | 35                      | 33     | 1030            | 1022     |
| 4w × 32s                   | 99 (needs 136)          | 90     | 2868            | **4059** |

Peak is the worst of three runs. An owned process holds exactly
`concurrency + 2`; the sampler counts every connection to the database, so a
straggler from the previous run can add one.

The last row is the one that matters. Owning connections, 4×32 needs 136
against a `max_connections` of 100, so slots that couldn't connect died at
startup and the rest ran short-handed. Pooled, it peaked at 90 and fit, and
throughput is 42% higher.

**The pool lowers steady use, not the peak.** At 1×32 only about 10
connections are busy on average, but the peak barely moved (33 against 35).
All 32 slots finish their first jobs together, the pool grows to meet that
burst, and it keeps what it opened until those connections have sat idle for
10 minutes. How far it grows varies from run to run: the first measurement of
this table saw 20, which is what this section originally quoted. So budget
`max_connections` for the worst case, `concurrency + 2` per process, pooled or
not. A real ceiling would mean capping the pool below `concurrency`, so that a
burst waits for connections instead of opening them.

**Why ~10 busy and not ~2?** A borrowed connection is held until its commit is
flushed, and fsync here is slow: in a separate run with
`synchronous_commit = off`, the average fell to 2.5. Completions lining up
because of the fixed sleep isn't the cause either: `io_job_varied`, which
spreads the wait over 10–30ms, averages the same (10.4 busy against 10.2).

**What it costs where it can't help.** `bench_job` needs its connection
throughout, so connections are unchanged (10 at 1×8, 40 at 4×8), and so is
throughput: 1586 owned against 1590 pooled at 1×8, 4217 against 4223 at 4×8.
Pre-opening the pool to full size changed nothing either.

A side effect worth having: a slot's dropped connection used to kill the
slot, because recording the failure needed the same dead connection. The
pool discards it, so the job costs one retry and the slot carries on.

### What didn't help

Three more attempts at raising throughput, none kept. Each was measured over
four or five interleaved rounds, with the checkpoint fix described below but
before timing moved to the database's clock:

- **Pipeline mode**, to send BEGIN, the task's SQL and the completion in one
  go instead of four round trips. It first exposed a trap: mid-pipeline,
  `rowcount` reads −1, so the completion check would have reported every job
  as lost and silently rolled back its work. The check uses `RETURNING` now.
  Then under load it deadlocked, every time, about 1,800 jobs in: stack dumps
  showed every slot blocked in psycopg's pipeline wait, while Postgres sat
  waiting on the client. That held with and without the pool and with
  prepared statements off (psycopg 3.3.5 on Windows). The prize was real:
  adding one round trip per job costs 4% at 1×8 and 14% at 4×8.
- **Claim batches past 25.** 50 and 100 measure the same as 25 (4024 and 4015
  against 4003 at 4×8). Below 25 does cost: `run_worker`'s default of 10 is 4%
  slower at 4×8 and 7% at 1×8.
- **Pre-opening the pool** (above): no change.

### Durability

**Concurrency pays for durability.** Every commit needs a WAL flush, but
Postgres group-commits concurrent transactions into shared flushes, so the more
commits in flight, the more of the fsync each one avoids paying for
(`--suite durability`):

|                        | fully durable | workers relaxed | cost of durability |
| ---------------------- | ------------- | --------------- | ------------------ |
| 4 in flight (1w × 4s)  | 862           | 1878            | 54%                |
| 16 in flight (2w × 8s) | 2947          | 3802            | 22%                |
| 32 in flight (4w × 8s) | 4084          | 5032            | 19%                |
| 64 in flight (8w × 8s) | 5579          | 5848            | **5%**             |

So `durable_bookkeeping=False` matters far less than it first appears — at real
concurrency you get full fsync-per-commit durability for single-digit percent,
and the default should stay on.

`commit_delay` (group commit) buys nothing here and costs throughput (−10% at
500µs, −21% at 1000µs, −24% at 2000µs with 4 workers, `--suite commitdelay`),
because Postgres is _already_ group committing — that is exactly what the
54% → 5% fall above is.

**Which commits actually need to survive a crash?** A worker makes two per job —
the claim and the completion — and losing either is not data loss, it's a
re-run: the job reverts to `pending` and executes again, which is the
at-least-once path this queue already implements and tests. The commit that
genuinely must survive is the **enqueue**, because losing that means work was
requested and silently never happened — and that commit lives in the
application's transaction, not the worker's.

Since `synchronous_commit` is per-transaction, you can have both. Workers run
with `run_worker(durable_bookkeeping=False)`, enqueues stay fully durable, and
throughput roughly doubles at low concurrency (862 → 1878 at 4 in flight),
because worker commits vastly outnumber enqueue commits. At high concurrency
group commit has already taken most of that cost, as the table shows. Nothing the queue guarantees is weakened. The real cost is a higher
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
efficiency, durability cost — are the ones worth quoting.

**How each figure is taken.** Each throughput figure is the median of three
runs. Each run starts from an empty table and a forced checkpoint, is sized to
its concurrency so that it lasts several seconds, and is timed on the
database's own clock, from the enqueue's commit to the last job's
`finished_at`. Each of those fixes something that distorted earlier numbers:

- **The checkpoint.** Postgres's timed checkpoint fires every 5 minutes and
  takes 5–47s to complete here. Any run that overlapped it came out up to ~30%
  slow, whatever it was measuring: in one round, all four variants under test
  landed within 10% of each other, well below their usual figures. Forcing a
  checkpoint before each run cut run-to-run spread from ±14% to ±2–4% at 1×8.
- **The clock.** Timing used to stop when a 50ms poll noticed the queue was
  empty, so the 8-worker rows, which drained in under a second, were barely
  measured at all.
- **The median.** Even so, one run in several at 32–64 in flight still came out
  13–17% low while the others agreed. The likely cause is autovacuum, which ran
  on `jobs` 40 times across these benchmark sessions, up to once a minute.
  It stays on, because a real queue table is vacuumed constantly; the median
  just stops one unlucky run from setting a figure.

Measurement itself distorted the results five separate times here: the
checkpoint and the clock above, plus a per-job heartbeat connection, a seq-scan
progress query, and a `docker stats` sampler that starved the workers it was
watching. That's worth remembering when reading any benchmark.

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
measured sweet spot. Budget connections for the worst case, every slot
mid-transaction at once: a process can use up to `concurrency + 2`, so 8
workers × 8 slots is 80 against Postgres's default `max_connections` of 100.

Run as many schedulers as you like; they coordinate through row locks, so
there's no leader to lose.

Schedules run every N seconds or on the clock, and registering on every
startup is safe:

```python
from durable_queue.scheduler import daily, hourly, register_schedule

register_schedule(conn, "digest", "send_digests", {}, hourly(minute=0))
register_schedule(conn, "stripe", "reconcile_stripe", {}, daily("04:00", "America/New_York"))
register_schedule(conn, "streaming", "refresh_streaming", {},
                  daily("11:00", "America/New_York"), max_lateness_seconds=3600)
```

`daily` follows the local clock through daylight saving changes. A schedule
that missed runs while nothing was scheduling runs once, for the most recent,
instead of once per missed run back to back. `max_lateness_seconds` skips
even that one when it's too late to be useful.

Task options: raise `PermanentError` for a failure no retry can fix, and use
`@task(max_attempts=3, max_execution_seconds=60)` to override the defaults
for one task. `max_execution_seconds` is a lease ceiling, not a timeout:
Python can't kill a thread, so a task that overruns keeps running, but its
lease lapses and the job is recovered elsewhere.

### Behind a pooler, or on Neon

Workers `LISTEN`, which can't work through a transaction-mode pooler such as
PgBouncer or Neon's `-pooler` endpoint, and it fails silently there: no
wakeups, only polling. Keep the app on the pooler and give the queue a direct
connection with `DURABLE_QUEUE_DATABASE_URL` (falling back to `DATABASE_URL`),
or `run_worker(dsn=...)` / `run_scheduler(dsn=...)`.

Connections the server drops, as Neon does on every compute restart and scale
to zero, are reopened on their next use, with backoff. The claim connection
replays its `LISTEN`, a heartbeat or slot that fails to reach the database
carries on rather than dying, and the slots' pool checks each connection
before lending it out. Without that check, a connection that died idle in the
pool fails the job it is lent to, spending an attempt or re-running a task
that had already finished. It costs a round trip per borrow, and the
benchmark tables above predate it: with it, 4×8 measured 3,497 jobs/sec
against 4,108 without, and 1×8 measured 1,544 against 1,573 (medians of three
interleaved runs). `check_connections=False` turns it off where connections
never drop.

### Enqueueing from SQLAlchemy

An app whose ORM runs on psycopg2 can enqueue inside its own session's
transaction (`pip install durable-queue[sqlalchemy]`):

```python
from durable_queue.sqlalchemy import enqueue

db.add(entry)
enqueue(db, "notify_friends", {"user_id": uid})
db.commit()  # both land, or neither does
```

`enqueue_many` is there too. Workers still use psycopg 3.

### Inside your app, on a database that scales to zero

A worker that polls keeps a database like Neon awake, and billing, around the
clock. In idle mode it doesn't poll: it works out when a job next needs it (a
schedule, a retry, a lease to recover) and sleeps until then without sending a
query, closing its connections first if that's more than a minute off. The
database sleeps too. `Runner` puts a worker and scheduler in idle mode on
background threads of your app:

```python
from durable_queue.runner import Runner

runner = Runner(concurrency=4)

@asynccontextmanager
async def lifespan(app):
    runner.start()
    yield
    await asyncio.to_thread(runner.stop)  # unstarted jobs go back, running ones finish
```

Besides its own due times, it wakes when this process creates work: a schedule
firing, a retry, an enqueue through `durable_queue.sqlalchemy` once it
commits. An enqueue from another process waits for its next wake, since
`LISTEN` is off in idle mode.

## Known gaps

No structured logging, no real migrations, and no `durable-queue worker` /
`scheduler` CLI entry points. `stop=` and `Runner.stop()` shut down gracefully,
but `python -m durable_queue.worker` doesn't yet turn SIGTERM into one.
