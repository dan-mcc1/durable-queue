"""
Worker subprocess entrypoint with a configurable number of concurrency
slots. Run as `python -m tests.chaos.slots_worker <concurrency>` from
the repo root. Not meant to be imported.
"""
import sys

import tests.chaos.chaos_tasks  # noqa: F401 - import registers @tasks
from durable_queue.worker import run_worker

if __name__ == "__main__":
    run_worker(
        poll_interval=0.05,
        lease_seconds=30,
        concurrency=int(sys.argv[1]),
    )
