from prometheus_client import Counter, Gauge, Histogram, Summary

# --- Counters (Counts occurrences) ---
etl_counter = Counter(
    'pipedrive_etl_runs_total',
    'Total ETL executions initiated'
)
etl_failure_counter = Counter(
    'pipedrive_etl_failures_total',
    'Total ETL executions that failed critically'
)
records_processed_counter = Counter(
    'pipedrive_etl_records_processed_total',
    'Total number of records successfully processed and loaded/upserted'
)
api_errors_counter = Counter(
    'pipedrive_api_errors_total',
    'Total Pipedrive API errors encountered by type and status code',
    ['error_type', 'status_code']
)

# --- Gauges (Represents a value that can go up and down) ---
memory_usage_gauge = Gauge(
    'pipedrive_etl_process_memory_mbytes', 
    'Current memory usage of the ETL process in Megabytes'
)
batch_size_gauge = Gauge(
    'pipedrive_etl_batch_size',
    'Number of records in the current processing batch'
)

# --- Histograms (Tracks distribution of values, e.g., request durations) ---
etl_duration_hist = Histogram(
    'pipedrive_etl_duration_seconds',
    'Histogram of total ETL processing time in seconds',
    buckets=[10, 30, 60, 120, 300, 600, 1800, 3600]
)
db_operation_duration_hist = Histogram(
    'pipedrive_db_operation_duration_seconds',
    'Histogram of database operation durations (e.g., upsert, select)',
    ['operation'],
    buckets=[0.1, 0.5, 1, 5, 10, 30, 60, 120]
)
api_request_duration_hist = Histogram(
    'pipedrive_api_request_duration_seconds',
    'Histogram of Pipedrive API request durations',
    ['endpoint', 'method', 'status_code'], 
    buckets=[0.1, 0.5, 1, 2, 5, 10, 20, 30]
)

# --- Summaries (Tracks quantile distribution) ---
transform_duration_summary = Summary(
    'pipedrive_transform_batch_duration_seconds',
    'Summary of time spent transforming a batch of data'
)

# --- Counter for API token cost ---
pipedrive_api_token_cost_total = Counter(
    'pipedrive_api_token_cost_total',
    'Estimated total token cost consumed for Pipedrive API calls',
    ['endpoint']
)
