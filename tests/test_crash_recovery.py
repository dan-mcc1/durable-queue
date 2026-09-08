from durable_queue.jobs import claim_next_job, enqueue, extend_lease, reap_expired_jobs


def test_reap_expired_jobs_returns_expired_lease_to_pending(conn, worker_id):
    job_id = enqueue(conn, "some_task", {})
    conn.commit()
    # A negative lease claims the job already expired, without sleeping
    # in the test to wait for a real expiry.
    claim_next_job(conn, worker_id, lease_seconds=-1)

    reaped = reap_expired_jobs(conn)

    assert reaped == 1
    with conn.cursor() as cur:
        cur.execute(
            "SELECT status, locked_by, locked_until FROM jobs WHERE id = %s",
            (job_id,),
        )
        row = cur.fetchone()
    assert row["status"] == "pending"
    assert row["locked_by"] is None
    assert row["locked_until"] is None


def test_reap_expired_jobs_leaves_healthy_leases_alone(conn, worker_id):
    enqueue(conn, "some_task", {})
    conn.commit()
    claim_next_job(conn, worker_id, lease_seconds=60)

    assert reap_expired_jobs(conn) == 0


def test_extend_lease_pushes_locked_until_forward(conn, worker_id):
    job_id = enqueue(conn, "some_task", {})
    conn.commit()
    claim_next_job(conn, worker_id, lease_seconds=5)

    with conn.cursor() as cur:
        cur.execute("SELECT locked_until FROM jobs WHERE id = %s", (job_id,))
        before = cur.fetchone()["locked_until"]

    extend_lease(conn, job_id, worker_id, lease_seconds=60)

    with conn.cursor() as cur:
        cur.execute("SELECT locked_until FROM jobs WHERE id = %s", (job_id,))
        after = cur.fetchone()["locked_until"]

    assert after > before


def test_extend_lease_refuses_to_touch_a_job_it_does_not_own(conn, worker_id):
    job_id = enqueue(conn, "some_task", {})
    conn.commit()
    claim_next_job(conn, worker_id, lease_seconds=60)

    with conn.cursor() as cur:
        cur.execute("SELECT locked_until FROM jobs WHERE id = %s", (job_id,))
        before = cur.fetchone()["locked_until"]

    # A different worker_id must not be able to extend a lease it
    # doesn't hold, even though it names the same job_id.
    extend_lease(conn, job_id, "some-other-worker", lease_seconds=999)

    with conn.cursor() as cur:
        cur.execute("SELECT locked_until FROM jobs WHERE id = %s", (job_id,))
        after = cur.fetchone()["locked_until"]

    assert after == before
