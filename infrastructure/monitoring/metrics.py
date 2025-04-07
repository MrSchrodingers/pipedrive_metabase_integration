import os
import structlog
from prometheus_client import (
    Counter,
    Gauge,
    Histogram,
    Summary,
    REGISTRY,
    push_to_gateway as prometheus_push 
)

log = structlog.get_logger(__name__) 

# --- Counters (Counts occurrences) ---
etl_counter = Counter(
    'pipedrive_etl_runs_total',
    'Total ETL executions initiated',
    ['flow_type']
)
etl_failure_counter = Counter(
    'pipedrive_etl_failures_total',
    'Total ETL executions that failed critically',
    ['flow_type']
)
records_processed_counter = Counter(
    'pipedrive_etl_records_processed_total',
    'Total number of records successfully processed and loaded/upserted',
     ['flow_type']
)
api_errors_counter = Counter(
    'pipedrive_api_errors_total',
    'Total Pipedrive API errors encountered by type and status code',
    ['endpoint', 'error_type', 'status_code'] 
)

# --- Gauges (Represents a value that can go up and down) ---
memory_usage_gauge = Gauge(
    'pipedrive_etl_process_memory_mbytes',
    'Peak memory usage of the ETL process run in Megabytes',
     ['flow_type']
)
batch_size_gauge = Gauge(
    'pipedrive_etl_batch_size',
    'Number of records in the current processing batch',
    ['flow_type']
)
# Gauge para backfill
backfill_deals_remaining_gauge = Gauge(
    'pipedrive_backfill_deals_remaining_estimated',
    'Estimated number of deals remaining for stage history backfill'
)

# --- Histograms (Tracks distribution of values, e.g., request durations) ---
etl_duration_hist = Histogram(
    'pipedrive_etl_duration_seconds',
    'Histogram of total ETL processing time in seconds',
    ['flow_type'],
    buckets=[10, 30, 60, 120, 300, 600, 1800, 3600, 7200, 10800]
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
    buckets=[0.1, 0.5, 1, 2, 5, 10, 20, 30, 45, 60]
)

# --- Summaries (Tracks quantile distribution) ---
transform_duration_summary = Summary(
    'pipedrive_transform_batch_duration_seconds',
    'Summary of time spent transforming a batch of data',
     ['flow_type']
)

# --- Counter for API token cost ---
pipedrive_api_token_cost_total = Counter(
    'pipedrive_api_token_cost_total',
    'Estimated total token cost consumed for Pipedrive API calls',
    ['endpoint']
)

# --- Função para enviar métricas ao Pushgateway ---
PUSHGATEWAY_ADDRESS = os.getenv("PUSHGATEWAY_ADDRESS", "pushgateway:9091")
push_log = structlog.get_logger("push_metrics")

def push_metrics_to_gateway(job_name="pipedrive_etl_job", grouping_key=None):
    """
    Pushes metrics from the default registry to the Pushgateway.

    Args:
        job_name (str): The job label for the pushed metrics.
        grouping_key (dict, optional): A dictionary of labels to use for grouping
                                       metrics in the Pushgateway. Useful for
                                       differentiating instances of the same job
                                       if needed (e.g., using flow_run_id).
                                       Defaults to None.
    """
    try:
        push_log.info("Attempting to push metrics to Pushgateway...",
                      address=PUSHGATEWAY_ADDRESS,
                      job=job_name,
                      grouping_key=grouping_key)
        prometheus_push(
            gateway=PUSHGATEWAY_ADDRESS,
            job=job_name,
            registry=REGISTRY,
            grouping_key=grouping_key
        )
        push_log.info("Successfully pushed metrics to Pushgateway.")
    except Exception as push_err:
        push_log.error("Failed to push metrics to Pushgateway",
                       error=str(push_err),
                       address=PUSHGATEWAY_ADDRESS,
                       job=job_name,
                       exc_info=True)