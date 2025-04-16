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
PUSHGATEWAY_ADDRESS = os.getenv("PUSHGATEWAY_ADDRESS", "pushgateway:9091")
push_log = structlog.get_logger("push_metrics")

# --- Buckets ---
REQUEST_DURATION_BUCKETS = [0.1, 0.25, 0.5, 0.75, 1, 2.5, 5, 10, 30, 60, 120]
DB_LATENCY_BUCKETS = [0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10]
RECORD_COUNT_BUCKETS = [0, 10, 50, 100, 250, 500, 1000, 2500, 5000, 10000]
TOKEN_COST_BUCKETS = [10, 50, 100, 250, 500, 1000, 2500, 5000, 10000, 20000]
API_CALL_COUNT_BUCKETS = [1, 5, 10, 20, 50, 100, 250, 500]

# --- General ETL Counters ---
etl_runs_total = Counter(
    "pipedrive_etl_runs_total",
    "Total ETL flow executions initiated",
    ["flow_type"]
)
etl_run_failures_total = Counter(
    "pipedrive_etl_run_failures_total",
    "Total ETL flow executions that failed critically",
    ["flow_type"]
)
etl_records_fetched_total = Counter(
    "pipedrive_etl_records_fetched_total",
    "Total number of raw records fetched from the source API",
    ["flow_type"]
)
etl_record_processing_failures_total = Counter(
    "pipedrive_etl_record_processing_failures_total",
    "Total records that failed during processing stages (schema, domain, load)",
    ["flow_type", "failure_stage"] # failure_stage: 'schema', 'domain', 'enrich', 'load'
)
etl_records_loaded_total = Counter(
    "pipedrive_etl_records_loaded_total",
    "Total number of records successfully loaded/upserted into the target",
    ["flow_type"]
)
etl_batches_processed_total = Counter(
    "pipedrive_etl_batches_processed_total",
    "Total number of batches processed",
    ["flow_type"]
)
etl_empty_batches_total = Counter(
    "pipedrive_etl_empty_batches_total",
    "Total ETL batches that had no data after fetching/filtering",
    ["flow_type"]
)
etl_skipped_batches_total = Counter(
    "pipedrive_etl_skipped_batches_total",
    "Total batches skipped due to validation errors or other reasons before load",
    ["flow_type"]
)

# --- API Interaction Metrics ---
pipedrive_api_call_total = Counter(
    "pipedrive_api_calls_total",
    "Total Pipedrive API calls made",
    ["endpoint", "method", "status_code"]
)
pipedrive_api_errors_total = Counter(
    "pipedrive_api_errors_total",
    "Total Pipedrive API errors by type",
    ["endpoint", "error_type", "status_code"]
)
pipedrive_api_token_cost_total = Counter(
    "pipedrive_api_token_cost_total",
    "Estimated total token cost consumed for Pipedrive API calls",
    ["endpoint"]
)
pipedrive_api_request_duration_seconds = Histogram(
    "pipedrive_api_request_duration_seconds",
    "Pipedrive API request latency distribution",
    ["endpoint", "method", "status_code"],
    buckets=REQUEST_DURATION_BUCKETS
)
pipedrive_api_rate_limit_remaining = Gauge(
    "pipedrive_api_rate_limit_remaining",
    "Remaining API quota reported by Pipedrive headers",
    ["endpoint"]
)
pipedrive_api_calls_per_batch_hist = Histogram(
    "pipedrive_api_calls_per_batch",
    "Distribution of Pipedrive API calls made per ETL batch",
    ["flow_type"],
    buckets=API_CALL_COUNT_BUCKETS
)
pipedrive_etl_duration_seconds = Histogram(
    "pipedrive_etl_duration_seconds", 
    "Histogram of total ETL flow processing time in seconds",
    ["flow_type"],
    # Buckets em segundos: (10s, 30s, 1m, 2m, 5m, 10m, 30m, 1h, 2h, 3h)
    buckets=[10, 30, 60, 120, 300, 600, 1800, 3600, 7200, 10800]
)
# --- Cache Interaction Metrics ---
pipedrive_api_cache_hit_total = Counter(
    "pipedrive_cache_hit_total",
    "Cache hit count for Pipedrive related lookups",
    ["entity", "source"] # source: 'redis', 'memory', etc.
)
pipedrive_cache_miss_total = Counter(
    "pipedrive_cache_miss_total",
    "Cache miss count for Pipedrive related lookups",
    ["entity", "source"]
)
etl_cache_errors_total = Counter(
    "pipedrive_etl_cache_errors_total",
    "Errors encountered during cache operations",
    ["operation", "cache_key_type"] # operation: 'get', 'set', 'delete'
)

# --- Database Interaction Metrics ---
etl_db_operation_duration_seconds = Histogram(
    "pipedrive_etl_db_operation_duration_seconds",
    "Database operation latency distribution",
    ["operation"], # e.g., 'upsert_deals', 'update_history', 'lookup_fetch', 'schema_update'
    buckets=DB_LATENCY_BUCKETS
)
etl_db_batch_latency_seconds = Histogram(
    "pipedrive_etl_db_batch_latency_seconds",
    "Database batch operation latency distribution",
    ["operation", "batch_size"],
    buckets=REQUEST_DURATION_BUCKETS # Use request buckets for potentially longer ops
)
etl_db_errors_total = Counter(
    "pipedrive_etl_db_errors_total",
    "Total database errors encountered during ETL operations",
    ["flow_type", "operation", "error_code"]
)

# --- Data Flow & Processing Metrics ---
etl_batch_processing_phase_duration_seconds = Histogram(
    "pipedrive_etl_batch_processing_phase_duration_seconds",
    "Duration of specific phases within batch processing",
    ["flow_type", "phase"], # phase: 'schema_validation', 'domain_mapping', 'persist_mapping', 'enrichment', 'db_load'
    buckets=REQUEST_DURATION_BUCKETS
)
etl_lookup_enrichment_duration_seconds = Histogram(
    "pipedrive_etl_lookup_enrichment_duration_seconds",
    "Time spent enriching data with lookups",
    ["flow_type"],
    buckets=REQUEST_DURATION_BUCKETS
)
etl_loaded_records_per_batch_hist = Histogram(
    "pipedrive_etl_loaded_records_per_batch_hist",
    "Distribution of records loaded per batch",
    ["flow_type"],
    buckets=RECORD_COUNT_BUCKETS
)

# --- Data Quality & Validation Metrics ---
etl_batch_validation_errors_total = Counter(
    "pipedrive_etl_batch_validation_errors_total",
    "Total validation errors per batch processing stage",
    ["flow_type", "validation_stage", "error_detail"] # stage: 'schema', 'domain'; detail: field path or error type
)
etl_data_consistency_issues_found_total = Counter(
    "pipedrive_etl_data_consistency_issues_found_total",
    "Data consistency issues found by specific checks",
    ["flow_type", "check_name"] # e.g., 'date_order'
)
etl_schema_drift_detected_total = Counter(
    "pipedrive_etl_schema_drift_detected_total",
    "Total times dynamic schema changes (column additions) were detected and applied",
    ["flow_type"]
)

# --- Auxiliary Sync Flow Metrics ---
sync_runs_total = Counter(
    "pipedrive_aux_sync_runs_total",
    "Total auxiliary sync flow executions initiated",
    ["entity_type"]
)
sync_run_failures_total = Counter(
    "pipedrive_aux_sync_run_failures_total",
    "Total auxiliary sync flow executions that failed",
    ["entity_type"]
)
sync_records_upserted_total = Counter(
    "pipedrive_aux_sync_records_upserted_total",
    "Total records upserted during auxiliary sync flows",
    ["entity_type"]
)
sync_api_fetch_duration_seconds = Histogram(
    "pipedrive_aux_sync_api_fetch_duration_seconds",
    "Duration of API data fetching for auxiliary syncs",
    ["entity_type"],
    buckets=REQUEST_DURATION_BUCKETS
)
sync_db_upsert_duration_seconds = Histogram(
    "pipedrive_aux_sync_db_upsert_duration_seconds",
    "Duration of database upsert operations for auxiliary syncs",
    ["entity_type"],
    buckets=REQUEST_DURATION_BUCKETS
)

# --- Backfill Flow Metrics ---
backfill_changelog_api_errors_total = Counter(
    "pipedrive_backfill_changelog_api_errors_total",
    "Total API errors encountered while fetching deal changelogs during backfill"
)
backfill_db_update_errors_total = Counter(
    "pipedrive_backfill_db_update_errors_total",
    "Total database errors encountered while updating stage history during backfill"
)
backfill_deals_remaining_gauge = Gauge(
    "pipedrive_backfill_deals_remaining_estimated",
    "Estimated number of deals remaining for stage history backfill"
)

# --- Experiment Metrics ---
batch_experiment_runs_total = Counter(
    "pipedrive_batch_experiment_runs_total",
    "Total batch size experiment executions initiated",
    ["experiment", "batch_size", "flow_run_id"]
)
batch_experiment_best_score = Gauge(
    "pipedrive_batch_experiment_best_score",
    "Best score calculated in the batch size experiment",
    ["flow_run_id", "metric"] # metric: e.g., 'overall_score'
)
batch_experiment_success_rate = Gauge(
    "pipedrive_batch_experiment_success_rate",
    "Success rate observed for a specific batch size during experiment",
    ["batch_size", "flow_run_id"]
)

# --- System Health & Status Gauges ---
etl_process_memory_mbytes = Gauge(
    "pipedrive_etl_process_memory_mbytes",
    "Peak memory usage (MB) reported by the ETL process",
    ["flow_type"]
)
etl_process_cpu_percent = Gauge(
    "pipedrive_etl_process_cpu_percent",
    "CPU usage percentage reported by the ETL process",
    ["flow_type"]
)
etl_process_thread_count = Gauge(
    "pipedrive_etl_process_thread_count",
    "Number of active threads reported by the ETL process",
    ["flow_type"]
)
etl_process_disk_usage_bytes = Gauge(
    "pipedrive_etl_process_disk_usage_bytes",
    "Disk usage (bytes) reported by the ETL process for a specific mount point",
    ["mount_point"]
)
etl_last_successful_run_timestamp = Gauge(
    "pipedrive_etl_last_successful_run_timestamp",
    "Unix timestamp of the last successfully completed ETL flow run",
    ["flow_type"]
)
etl_heartbeat = Gauge(
    "pipedrive_etl_heartbeat",
    "Unix timestamp of the last heartbeat (run completion or check-in)",
    ["flow_type"]
)
etl_pushgateway_up = Gauge(
    "pipedrive_etl_pushgateway_up",
    "Indicates if the last attempt to push to Pushgateway was successful (1=OK, 0=Fail)",
    ["instance"]
)
etl_component_initialization_status = Gauge(
    "pipedrive_etl_component_initialization_status",
    "Status of component initialization (1=OK, 0=Fail)",
    ["component_name"]
)
batch_size_gauge = Gauge(
    "pipedrive_etl_batch_size",
    "Number of records in the current processing batch (gauge for current value)",
    ["flow_type"]
)


# --- Função de Push ---
def push_metrics_to_gateway(job_name="pipedrive_etl_job", grouping_key=None):
    try:
        push_log.info("Attempting to push metrics to Pushgateway...",
                      address=PUSHGATEWAY_ADDRESS,
                      job=job_name,
                      grouping_key=grouping_key
                      )
        prometheus_push(gateway=PUSHGATEWAY_ADDRESS,
                        job=job_name,
                        registry=REGISTRY,
                        grouping_key=grouping_key
                        )
        etl_pushgateway_up.labels(instance=PUSHGATEWAY_ADDRESS).set(1)
        push_log.info("Successfully pushed metrics to Pushgateway.")
    except Exception as push_err:
        etl_pushgateway_up.labels(instance=PUSHGATEWAY_ADDRESS).set(0)
        push_log.error("Failed to push metrics to Pushgateway",
                       error=str(push_err),
                       address=PUSHGATEWAY_ADDRESS,
                       job=job_name,
                       exc_info=True)