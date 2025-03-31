from prometheus_client import Counter, Gauge, Histogram

# Execution metrics
etl_counter = Counter('etl_runs_total', 'Total ETL executions')
etl_failure_counter = Counter('etl_failures_total', 'ETL failures count')
etl_duration = Histogram('etl_duration_seconds', 'ETL processing time', buckets=[1, 5, 10, 30, 60, 120])

# Resource metrics
memory_usage = Gauge('process_memory_bytes', 'Memory usage in bytes')
batch_size = Gauge('etl_batch_size', 'Current processing batch size')
records_processed = Counter('records_processed_total', 'Total processed records')

# Performance metrics
insert_duration = Histogram(
    'db_insert_duration_seconds',
    'Tempo gasto nas operações de inserção no banco de dados',
    buckets=[0.1, 0.5, 1, 2, 5, 10]
)
api_request_duration = Histogram('api_request_duration_seconds', 'Pipedrive API request time')