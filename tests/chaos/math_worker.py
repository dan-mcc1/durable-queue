"""
Standalone entrypoint for a fan-out/combine demo worker subprocess. Run
as `python -m tests.chaos.math_worker` (with cwd at the repo root).
Not meant to be imported - only launched via subprocess.
"""
import tests.chaos.math_tasks  # noqa: F401 - import registers @tasks
from durable_queue.worker import run_worker

if __name__ == "__main__":
    run_worker(poll_interval=0.05, lease_seconds=2)
