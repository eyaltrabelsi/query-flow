ALTER SYSTEM SET work_mem='0.5GB';
ALTER SYSTEM SET effective_cache_size='16GB';
ALTER SYSTEM SET shared_buffers='8GB';
ALTER SYSTEM SET max_parallel_workers=8;
ALTER SYSTEM SET max_parallel_workers_per_gather=4;
ALTER SYSTEM SET max_connections=25;
ALTER SYSTEM SET maintenance_work_mem='1.5GB';
ALTER SYSTEM SET enable_partitionwise_aggregate='on';
ALTER SYSTEM SET enable_partitionwise_join='on';
ALTER SYSTEM SET log_min_duration_statement=0;
ALTER SYSTEM SET max_locks_per_transaction=1024;
ALTER SYSTEM SET constraint_exclusion = on;