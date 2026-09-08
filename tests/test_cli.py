from typer.testing import CliRunner

from durable_queue.cli import app
from durable_queue.jobs import enqueue

runner = CliRunner()


def test_stats_runs_against_an_empty_queue(conn):
    result = runner.invoke(app, ["stats"])

    assert result.exit_code == 0
    assert "pending:   0" in result.stdout
    assert "oldest due pending job: none" in result.stdout


def test_ls_lists_an_enqueued_job(conn):
    enqueue(conn, "some_task", {})
    conn.commit()

    result = runner.invoke(app, ["ls"])

    assert result.exit_code == 0
    assert "some_task" in result.stdout


def test_show_reports_missing_job_with_nonzero_exit(conn):
    result = runner.invoke(app, ["show", "999999"])

    assert result.exit_code == 1
    assert "No job with id 999999" in result.stdout


def test_retry_reports_failure_for_a_non_dead_job(conn):
    job_id = enqueue(conn, "some_task", {})
    conn.commit()

    result = runner.invoke(app, ["retry", str(job_id)])

    assert result.exit_code == 1
    assert "nothing to retry" in result.stdout
