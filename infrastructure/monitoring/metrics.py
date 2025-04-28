import os
import structlog
import psutil
from prometheus_client import (
    Counter,
    Gauge,
    Histogram,
    Summary,
    REGISTRY,
    push_to_gateway as prometheus_push,
    PROCESS_COLLECTOR,
    GC_COLLECTOR,
    PLATFORM_COLLECTOR,
)

log = structlog.get_logger(__name__)
PUSHGATEWAY_ADDRESS = os.getenv("PUSHGATEWAY_ADDRESS", "pushgateway:9091")
push_log = structlog.get_logger("push_metrics")

# --- Counters ---
etl_counter = Counter("pipedrive_etl_runs_total", "Total ETL executions initiated", ["flow_type"])
etl_failure_counter = Counter("pipedrive_etl_failures_total", "Total ETL executions that failed critically", ["flow_type"])
records_processed_counter = Counter("pipedrive_etl_records_processed_total", "Total number of records successfully processed and loaded/upserted", ["flow_type"])
etl_empty_batches_total = Counter("pipedrive_etl_empty_batches_total", "Total ETL batches that had no data", ["flow_type"])
etl_skipped_batches_total = Counter("etl_skipped_batches_total", "Total batches skipped during ETL", ["flow_type"])
etl_batch_validation_errors_total = Counter("pipedrive_etl_batch_validation_errors_total", "Total validation or transformation errors per batch", ["flow_type", "error_type"])
etl_data_quality_issues_total = Counter("etl_data_quality_issues_total", "Data quality issues detected", ["flow_type", "field_name", "issue_type"])
etl_final_column_mismatch_total = Counter("etl_final_column_mismatch_total", "Total times ETL found column mismatch and fixed schema dynamically", ["flow_type"])
etl_extract_failures_total = Counter("etl_extract_failures_total", "Failures during ETL extraction phase", ["flow_type"])
etl_transform_failures_total = Counter("etl_transform_failures_total", "Failures during ETL transformation phase", ["flow_type"])
etl_load_failures_total = Counter("etl_load_failures_total", "Failures during ETL load phase", ["flow_type"])

# API related
pipedrive_api_call_total       = Counter("pipedrive_api_call_total",       "Total Pipedrive API calls", ["endpoint", "method", "status_code"])
pipedrive_api_token_cost_total = Counter("pipedrive_api_token_cost_total", "Estimated total token cost consumed for Pipedrive API calls", ["endpoint"])
api_errors_counter             = Counter("pipedrive_api_errors_total",     "Total API errors by type", ["endpoint", "error_type", "status_code"])
pipedrive_api_cache_hit_total  = Counter("pipedrive_api_cache_hit_total",  "Cache hit rate for Pipedrive lookups", ["entity", "source"])
pipedrive_api_cache_miss_total = Counter("pipedrive_api_cache_miss_total", "Cache miss rate for Pipedrive lookups", ["entity", "source"])
pipedrive_api_retries_total    = Counter("pipedrive_api_retries_total",    "Total retries attempted for Pipedrive API calls", ["endpoint"])
pipedrive_api_retry_failures_total = Counter("pipedrive_api_retry_failures_total", "Failures after retry attempts for Pipedrive API calls", ["endpoint"])

# --- Gauges ---
memory_usage_gauge                          = Gauge("pipedrive_etl_process_memory_mbytes",      "Peak memory usage of the ETL process run in Megabytes", ["flow_type"])
etl_heartbeat                               = Gauge("etl_heartbeat",                            "Timestamp da última execução do flow", ["flow_type"])
etl_cpu_usage_percent                       = Gauge("etl_cpu_usage_percent",                      "CPU usage percentage of ETL process", ["flow_type"])
etl_thread_count                            = Gauge("etl_thread_count",                          "Number of active threads in ETL process", ["flow_type"])
etl_disk_usage_bytes                        = Gauge("etl_disk_usage_bytes",                       "Disk usage in bytes for ETL storage", ["mount_point"])
etl_pushgateway_up                          = Gauge("etl_pushgateway_up",                         "Status of pushgateway success (1 if ok)", ["instance"])
db_active_connections                       = Gauge("db_active_connections",                      "Número de conexões ativas no pool de banco")
db_idle_connections                         = Gauge("db_idle_connections",                        "Número de conexões ociosas no pool de banco")
backfill_deals_remaining_gauge              = Gauge("pipedrive_backfill_deals_remaining_estimated","Estimated number of deals remaining for stage history backfill")
etl_last_successful_run_timestamp           = Gauge("etl_last_successful_run_timestamp",          "Timestamp (UNIX) da última execução bem-sucedida do ETL", ["flow_type"])
etl_transformation_error_rate               = Gauge("etl_transformation_error_rate",              "Taxa de erro durante transformação Pydantic + transformação pandas", ["flow_type"])
batch_size_gauge                            = Gauge("pipedrive_etl_batch_size",                    "Number of records in the current processing batch", ["flow_type"])
pipedrive_api_rate_limit_remaining          = Gauge("pipedrive_api_rate_limit_remaining",          "Remaining API quota before hitting rate limit", ["endpoint"])
pipedrive_api_rate_limit_reset_seconds      = Gauge("pipedrive_api_rate_limit_reset_seconds",      "Seconds until API rate limit resets", ["endpoint"])

# --- Histograms ---
etl_duration_hist               = Histogram("pipedrive_etl_duration_seconds",     "Total ETL processing time", ["flow_type"], buckets=[10,30,60,120,300,600,1800,3600,7200,10800])
hist_extract                    = Histogram("etl_extract_seconds",              "Tempo de extração no ETL", ["flow_type"])
hist_transform                  = Histogram("etl_transform_seconds",            "Tempo de transformação no ETL", ["flow_type"])
hist_load                       = Histogram("etl_load_seconds",                 "Tempo de carga no ETL", ["flow_type"])
db_operation_duration_hist      = Histogram("pipedrive_db_operation_duration_seconds","DB operation durations", ["operation"], buckets=[0.1,0.5,1,5,10,30,60,120])
etl_loaded_records_per_batch    = Histogram("etl_loaded_records_per_batch", "Distribuição do número de registros carregados por batch", ["flow_type"], buckets=[0, 10, 50, 100, 200, 500, 1000, 2000])
api_request_duration_hist       = Histogram("pipedrive_api_request_duration_seconds", "Pipedrive API request durations", ["endpoint", "method", "status_code"], buckets=[0.1, 0.5, 1, 2, 5, 10, 20, 30, 45, 60])

# --- Summaries ---
transform_duration_summary = Summary("pipedrive_transform_batch_duration_seconds", "Time spent transforming a batch of data", ["flow_type"])

# --- Experiment Counters / Gauges ---
batch_experiment_counter     = Counter("pipedrive_batch_experiment_runs_total",   "Batch experiment executions", ["experiment","batch_size","flow_run_id"])
batch_experiment_best_score  = Gauge("batch_experiment_best_score",  "Melhor score encontrado no experimento de batch", ["flow_run_id","metric"])
batch_experiment_success_rate= Gauge("batch_experiment_success_rate","Taxa de sucesso dos experimentos por batch size", ["batch_size","flow_run_id"])

# Sync flows
sync_counter            = Counter("pipedrive_aux_sync_runs_total", "Total de auxiliary syncs executados", ["entity_type"])
sync_failure_counter    = Counter("pipedrive_aux_sync_failures_total", "Total de auxiliary syncs que falharam", ["entity_type"])
records_synced_counter  = Counter("pipedrive_aux_sync_records_synced_total", "Total de registros sincronizados", ["entity_type"])

# --- Função de Push ---
# --- Função de Push ---
# infrastructure/monitoring/metrics.py
# -----------------------------------
def push_metrics_to_gateway(
    flow_name: str | None = None,
    *,
    job_name: str = "pipedrive_metrics",
    grouping_key: dict[str, str] | None = None,
) -> None:
    """
    Envia métricas ao Pushgateway.

    • `job_name` fica fixo em "pipedrive_metrics"                      (obrigatório p/ dashboard legado)
    • `flow` é adicionado automaticamente (usa `flow_name` ou agrupa em "unknown")
    • Qualquer `grouping_key` extra passado pelo chamador é mesclado
      (ex.: {'flow_run_id': 'xyz'}).

    Assim chamadas antigas funcionam:

        push_metrics_to_gateway(job_name="batch_experiment",
                                grouping_key={'flow_run_id': id})

    …e chamadas novas podem usar só o `flow_name`:

        push_metrics_to_gateway(flow_name="pipedrive_sync_stages_pipelines")
    """
    # 1) normaliza arguments ---------------------------------------------------
    if grouping_key is None:
        grouping_key = {}

    if flow_name is None and "flow" not in grouping_key:
        flow_name = "unknown"

    if flow_name is not None:
        grouping_key = {"flow": flow_name, **grouping_key}

    job_name_fixed = "pipedrive_metrics"

    # 2) push ------------------------------------------------------------------
    try:
        push_log.info(
            "Pushing metrics",
            address=PUSHGATEWAY_ADDRESS,
            job=job_name_fixed,
            grouping_key=grouping_key,
        )
        prometheus_push(
            gateway=PUSHGATEWAY_ADDRESS,
            job=job_name_fixed,
            registry=REGISTRY,
            grouping_key=grouping_key,
        )
        etl_pushgateway_up.labels(instance=PUSHGATEWAY_ADDRESS).set(1)
    except Exception as e:
        etl_pushgateway_up.labels(instance=PUSHGATEWAY_ADDRESS).set(0)
        push_log.error("Failed to push metrics", error=str(e), exc_info=True)

