import typer

from durable_queue.db import get_connection
from durable_queue.jobs import get_job, get_queue_stats, list_jobs, retry_job

app = typer.Typer()


@app.command()
def ls(status: str = typer.Option(None, help="Filter by status: pending, running, succeeded, dead"),
       limit: int = typer.Option(20, help="Max rows to show")) -> None:
    """List jobs, most recent first."""
    conn = get_connection()
    try:
        jobs = list_jobs(conn, status=status, limit=limit)
    finally:
        conn.close()
    if not jobs:
        typer.echo("No jobs found.")
        return
    for job in jobs:
        typer.echo(
            f"{job['id']:>6}  {job['status']:<10} {job['task']:<30} "
            f"attempts={job['attempts']} created_at={job['created_at']}"
        )


@app.command()
def show(job_id: int) -> None:
    """Show full detail for one job."""
    conn = get_connection()
    try:
        job = get_job(conn, job_id)
    finally:
        conn.close()
    if job is None:
        typer.echo(f"No job with id {job_id}.")
        raise typer.Exit(code=1)
    for key, value in job.items():
        typer.echo(f"{key}: {value}")


@app.command()
def retry(job_id: int) -> None:
    """Reset a dead job back to pending, with a clean attempts count."""
    conn = get_connection()
    try:
        succeeded = retry_job(conn, job_id)
    finally:
        conn.close()
    if succeeded:
        typer.echo(f"Job {job_id} reset to pending.")
    else:
        typer.echo(f"Job {job_id} is not dead (or doesn't exist) - nothing to retry.")
        raise typer.Exit(code=1)


@app.command()
def dead(limit: int = typer.Option(20, help="Max rows to show")) -> None:
    """List dead jobs."""
    conn = get_connection()
    try:
        jobs = list_jobs(conn, status="dead", limit=limit)
    finally:
        conn.close()
    if not jobs:
        typer.echo("No dead jobs.")
        return
    for job in jobs:
        typer.echo(
            f"{job['id']:>6}  {job['task']:<30} attempts={job['attempts']} "
            f"last_error={job['last_error']}"
        )


@app.command()
def stats() -> None:
    """Show queue counts and the oldest-due-pending-job age - the number worth alerting on."""
    conn = get_connection()
    try:
        s = get_queue_stats(conn)
    finally:
        conn.close()
    typer.echo(f"pending:   {s['pending']}")
    typer.echo(f"running:   {s['running']}")
    typer.echo(f"succeeded: {s['succeeded']}")
    typer.echo(f"dead:      {s['dead']}")
    if s["oldest_pending_age_seconds"] is None:
        typer.echo("oldest due pending job: none")
    else:
        typer.echo(f"oldest due pending job: {s['oldest_pending_age_seconds']:.1f}s old")


if __name__ == "__main__":
    app()
