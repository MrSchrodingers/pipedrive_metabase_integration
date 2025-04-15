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

# --- Counters ---
etl_counter = Counter("pipedrive_etl_runs_total", "Total ETL executions initiated", ["flow_type"])
etl_failure_counter = Counter("pipedrive_etl_failures_total", "Total ETL executions that failed critically", ["flow_type"])
records_processed_counter = Counter("pipedrive_etl_records_processed_total", "Total number of records successfully processed and loaded/upserted", ["flow_type"])
etl_empty_batches_total = Counter("pipedrive_etl_empty_batches_total", "Total ETL batches that had no data", ["flow_type"])
etl_batch_validation_errors_total = Counter("pipedrive_etl_batch_validation_errors_total", "Total validation or transformation errors per batch", ["flow_type", "error_type"])
etl_data_quality_issues_total = Counter("etl_data_quality_issues_total", "Data quality issues detected", ["flow_type", "field_name", "issue_type"])
etl_final_column_mismatch_total = Counter("etl_final_column_mismatch_total", "Total times ETL found column mismatch and fixed schema dynamically", ["flow_type"])

# API related
pipedrive_api_token_cost_total = Counter("pipedrive_api_token_cost_total", "Estimated total token cost consumed for Pipedrive API calls", ["endpoint"])
pipedrive_api_call_total = Counter("pipedrive_api_call_total", "Total Pipedrive API calls", ["endpoint", "method", "status_code"])
api_errors_counter = Counter("pipedrive_api_errors_total", "Total API errors by type", ["endpoint", "error_type", "status_code"])
pipedrive_api_cache_hit_total = Counter("pipedrive_api_cache_hit_total", "Cache hit rate for Pipedrive lookups", ["entity", "source"])

etl_skipped_batches_total = Counter("etl_skipped_batches_total", "Total batches skipped during ETL", ["flow_type"])

# Sync flows
sync_counter = Counter("pipedrive_aux_sync_runs_total", "Total auxiliary sync executions initiated", ["entity_type"])
sync_failure_counter = Counter("pipedrive_aux_sync_failures_total", "Total auxiliary sync executions that failed", ["entity_type"])
records_synced_counter = Counter("pipedrive_aux_sync_records_synced_total", "Total records upserted during auxiliary sync", ["entity_type"])

# Experiment
batch_experiment_counter = Counter("pipedrive_batch_experiment_runs_total", "Total batch size experiment executions", ["experiment", "batch_size", "flow_run_id"])

# --- Gauges ---
memory_usage_gauge = Gauge("pipedrive_etl_process_memory_mbytes", "Peak memory usage of the ETL process run in Megabytes", ["flow_type"])
etl_heartbeat = Gauge("etl_heartbeat", "Timestamp da última execução do flow", ["flow_type"])
etl_cpu_usage_percent = Gauge("etl_cpu_usage_percent", "CPU usage percentage of ETL process", ["flow_type"])
etl_thread_count = Gauge("etl_thread_count", "Number of active threads in ETL process", ["flow_type"])
etl_disk_usage_bytes = Gauge("etl_disk_usage_bytes", "Disk usage in bytes for ETL storage", ["mount_point"])
etl_pushgateway_up = Gauge("etl_pushgateway_up", "Status of pushgateway success (1 if ok)", ["instance"])

batch_size_gauge = Gauge("pipedrive_etl_batch_size", "Number of records in the current processing batch", ["flow_type"])
backfill_deals_remaining_gauge = Gauge("pipedrive_backfill_deals_remaining_estimated", "Estimated number of deals remaining for stage history backfill")

etl_last_successful_run_timestamp = Gauge("etl_last_successful_run_timestamp", "Timestamp (UNIX) da última execução bem-sucedida do ETL", ["flow_type"])
pipedrive_api_rate_limit_remaining = Gauge("pipedrive_api_rate_limit_remaining", "Remaining API quota before hitting rate limit", ["endpoint"])
etl_transformation_error_rate = Gauge("etl_transformation_error_rate", "Taxa de erro durante transformação Pydantic + transformação pandas", ["flow_type"])
etl_cache_hit_ratio = Gauge("etl_cache_hit_ratio", "Proporção de acertos de cache em lookups", ["entity"])

# --- Histograms ---
etl_duration_hist = Histogram("pipedrive_etl_duration_seconds", "Histogram of total ETL processing time in seconds", ["flow_type"], buckets=[10, 30, 60, 120, 300, 600, 1800, 3600, 7200, 10800])
db_operation_duration_hist = Histogram("pipedrive_db_operation_duration_seconds", "Histogram of DB operation durations", ["operation"], buckets=[0.1, 0.5, 1, 5, 10, 30, 60, 120])
db_batch_latency = Histogram("db_batch_operation_latency_seconds", "Latency of DB batch operations", ["operation", "batch_size"])
api_request_duration_hist = Histogram("pipedrive_api_request_duration_seconds", "Pipedrive API request durations", ["endpoint", "method", "status_code"], buckets=[0.1, 0.5, 1, 2, 5, 10, 20, 30, 45, 60])
etl_loaded_records_per_batch = Histogram("etl_loaded_records_per_batch", "Distribuição do número de registros carregados por batch", ["flow_type"], buckets=[0, 10, 50, 100, 200, 500, 1000, 2000])
pipedrive_token_cost_per_flow_run = Histogram("pipedrive_token_cost_per_flow_run", "Custo em tokens por execução do flow", ["flow_type"], buckets=[10, 50, 100, 200, 500, 1000, 5000, 10000])
pipedrive_token_cost_per_record = Histogram("pipedrive_token_cost_per_record", "Custo médio de token por registro processado", ["endpoint"], buckets=[0.1, 0.5, 1, 2, 5, 10])
pipedrive_api_calls_per_batch = Histogram("pipedrive_api_calls_per_batch", "Número de chamadas API por batch do ETL", ["flow_type"], buckets=[1, 5, 10, 20, 50, 100])

# --- Summaries ---
transform_duration_summary = Summary("pipedrive_transform_batch_duration_seconds", "Time spent transforming a batch of data", ["flow_type"])

# --- Experiment Gauges ---
batch_experiment_best_score = Gauge("batch_experiment_best_score", "Melhor score encontrado no experimento de batch", ["flow_run_id", "metric"])
batch_experiment_success_rate = Gauge("batch_experiment_success_rate", "Taxa de sucesso dos experimentos por batch size", ["batch_size", "flow_run_id"])

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
