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

## What's built

| | |
|---|---|
| **Atomic claiming** | `FOR UPDATE SKIP LOCKED`, so N workers never hand out the same job twice |
| **Crash recovery** | Leases + heartbeats + a reaper, with a ceiling so a hung task can't hold its lease forever |
| **Retries** | Exponential backoff with full jitter; poison pills that crash their worker are dead-lettered by recovery count |
| **Idempotency** | Enqueue-time dedup via `idempotency_key`, effect-time dedup via an effects ledger |
| **Scheduling** | Recurring schedules with `pg_try_advisory_lock` leader election, plus a per-run idempotency backstop |
| **Observability** | `durable-queue ls / show / retry / dead / stats` |
| **Chaos tests** | Real worker subprocesses killed mid-job, asserting invariants across many jobs and many kills |

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
