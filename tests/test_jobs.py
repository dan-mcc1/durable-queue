from durable_queue.jobs import claim_next_job, enqueue, mark_succeeded


def test_enqueue_creates_pending_row(conn):
    job_id = enqueue(conn, "some_task", {"x": 1})
    conn.commit()

    with conn.cursor() as cur:
        cur.execute("SELECT task, args, status FROM jobs WHERE id = %s", (job_id,))
        row = cur.fetchone()

    assert row["task"] == "some_task"
    assert row["args"] == {"x": 1}
    assert row["status"] == "pending"


def test_enqueue_returns_existing_id_for_duplicate_idempotency_key(conn):
    first_id = enqueue(conn, "some_task", {}, idempotency_key="dup-key")
    conn.commit()

    second_id = enqueue(conn, "some_task", {}, idempotency_key="dup-key")
    conn.commit()

    assert second_id == first_id
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) AS n FROM jobs WHERE idempotency_key = %s", ("dup-key",))
        assert cur.fetchone()["n"] == 1


def test_enqueue_allows_multiple_null_idempotency_keys(conn):
    id1 = enqueue(conn, "some_task", {})
    id2 = enqueue(conn, "some_task", {})
    conn.commit()

    assert id1 != id2


def test_claim_next_job_returns_none_when_queue_is_empty(conn, worker_id):
    assert claim_next_job(conn, worker_id) is None


def test_claim_next_job_marks_row_running_and_returns_it(conn, worker_id):
    job_id = enqueue(conn, "some_task", {"y": 2})
    conn.commit()

    claimed = claim_next_job(conn, worker_id)

    assert claimed["id"] == job_id
    assert claimed["task"] == "some_task"
    assert claimed["args"] == {"y": 2}

    with conn.cursor() as cur:
        cur.execute("SELECT status, locked_by FROM jobs WHERE id = %s", (job_id,))
        row = cur.fetchone()
    assert row["status"] == "running"
    assert row["locked_by"] == worker_id


def test_claim_next_job_does_not_reclaim_a_running_job(conn, worker_id):
    enqueue(conn, "some_task", {})
    conn.commit()

    first = claim_next_job(conn, worker_id)
    second = claim_next_job(conn, worker_id)

    assert first is not None
    assert second is None


def test_claim_next_job_orders_oldest_first(conn, worker_id):
    # Both rows get the same run_at here: now() is fixed for the whole
    # transaction in Postgres, not re-evaluated per statement, so these
    # two enqueue calls (before commit) produce identical timestamps.
    # That makes this test a direct check that "ORDER BY run_at, id"
    # actually needs the id tiebreak to stay deterministic.
    older_id = enqueue(conn, "some_task", {"order": "first"})
    enqueue(conn, "some_task", {"order": "second"})
    conn.commit()

    claimed = claim_next_job(conn, worker_id)

    assert claimed["id"] == older_id


def test_mark_succeeded_sets_status_and_finished_at(conn, worker_id):
    job_id = enqueue(conn, "some_task", {})
    conn.commit()
    claim_next_job(conn, worker_id)

    mark_succeeded(conn, job_id, worker_id)

    with conn.cursor() as cur:
        cur.execute("SELECT status, finished_at FROM jobs WHERE id = %s", (job_id,))
        row = cur.fetchone()
    assert row["status"] == "succeeded"
    assert row["finished_at"] is not None
