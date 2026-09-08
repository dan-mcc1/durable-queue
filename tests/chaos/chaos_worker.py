"""
Standalone entrypoint for a chaos-test worker subprocess. Run as
`python -m tests.chaos.chaos_worker` (with cwd at the repo root) so that
both `tests.chaos.chaos_tasks` and the installed `durable_queue` package
resolve. Not meant to be imported - only launched via subprocess.
"""
import tests.chaos.chaos_tasks  # noqa: F401 - import registers @task
from durable_queue.worker import run_worker

if __name__ == "__main__":
    # Short lease and fast polling: a killed job's lease needs to expire
    # and get reaped quickly, or the test spends most of its time waiting
    # rather than exercising anything.
    run_worker(poll_interval=0.05, lease_seconds=2)
