"""
Placeholder for the M10 load test: drive sustained load through the
queue, measure claim latency over time, and watch it degrade as dead
tuples accumulate and autovacuum falls behind. Diagnose with
n_dead_tup in pg_stat_user_tables and pgstattuple, then fix by
archiving completed rows, partitioning, or per-table autovacuum
tuning - and publish both curves.

Not implemented yet. (This file previously held a stale duplicate of
tests/test_worker.py, which was neither a load test nor current.)
"""
