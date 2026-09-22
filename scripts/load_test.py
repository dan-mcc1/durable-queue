"""
Sustained-load harness: drive the queue continuously and track claim
latency as dead tuples accumulate and autovacuum falls behind.
Diagnosed with n_dead_tup in pg_stat_user_tables and pgstattuple;
mitigated by archiving completed rows, partitioning, or per-table
autovacuum tuning.

Not implemented.
"""
