import structlog
from prefect import flow, task
from prefect.blocks.system import JSON
import os
import logging

from infrastructure.logging_config import setup_logging

log = structlog.get_logger(__name__)
log = structlog.get_logger() 

DEFAULT_BATCH_SIZE = 1000
DEFAULT_RETRIES = 10
DEFAULT_RETRY_DELAY = 90
DEFAULT_TIMEOUT = 7200 

@task(
    name="Execute Pipedrive ETL Task",
    retries=DEFAULT_RETRIES,
    retry_delay_seconds=DEFAULT_RETRY_DELAY,
    log_prints=True,
    timeout_seconds=DEFAULT_TIMEOUT
)
def execute_etl_task(batch_size: int = DEFAULT_BATCH_SIZE) -> dict:
    """
    Prefect task to initialize and run the ETL service.
    Handles configuration loading and dependency injection.
    """
    task_log = log.bind(task_name="execute_etl_task", batch_size=batch_size)
    task_log.info("Starting ETL task execution.")

    try:
        # --- Configuration Loading ---
        task_log.debug("Loading configuration blocks.")
        postgres_config = JSON.load("postgres-pool").value
        redis_config = JSON.load("redis-cache").value

        # --- Dependency Injection ---
        task_log.debug("Initializing infrastructure components.")

        from infrastructure.db_pool import DBConnectionPool
        db_pool = DBConnectionPool(
            minconn=postgres_config.get("minconn", 2), 
            maxconn=postgres_config.get("maxconn", 10),
            dsn=postgres_config["dsn"]
        )

        # Redis Cache
        from infrastructure.cache import RedisCache
        redis_cache = RedisCache(connection_string=redis_config["connection_string"])

        # Pipedrive API Client
        from infrastructure.api_clients.pipedrive_api_client import PipedriveAPIClient
        pipedrive_client = PipedriveAPIClient(cache=redis_cache) 

        # Fetch mapping needed by Repository
        try:
            custom_mapping = pipedrive_client.fetch_deal_fields_mapping()
            if not custom_mapping:
                 task_log.warning("Custom field mapping is empty. Custom fields may not be processed correctly.")
        except Exception as mapping_err:
             task_log.error("Failed to fetch initial custom field mapping. ETL might be incomplete.", error=str(mapping_err))
             raise RuntimeError(f"Failed to fetch Pipedrive field mapping: {mapping_err}") from mapping_err


        # Data Repository
        from infrastructure.repository_impl.pipedrive_repository import PipedriveRepository
        repository = PipedriveRepository(db_pool=db_pool, custom_field_api_mapping=custom_mapping)

        # ETL Service
        from application.services.etl_service import ETLService
        etl_service = ETLService(
            client=pipedrive_client,
            repository=repository,
            batch_size=batch_size
        )

        # --- Run ETL ---
        task_log.info("Running the ETL service.")
        result = etl_service.run_etl()
        task_log.info("ETL service run completed.")
        
        # --- Push Metrics to Pushgateway ---
        try:
            pushgateway_address = os.getenv("PUSHGATEWAY_ADDRESS", "pushgateway:9091")
            job_name = "pipedrive_etl_job"
            
            from prometheus_client import REGISTRY, push_to_gateway
            
            push_to_gateway(pushgateway_address, job=job_name, registry=REGISTRY)
            task_log.info("Successfully pushed metrics to Pushgateway", address=pushgateway_address, job=job_name)
        except Exception as push_err:
            task_log.error("Failed to push metrics to Pushgateway", error=str(push_err), exc_info=True)

        return result

    except Exception as e:
        task_log.critical("ETL task failed critically.", error=str(e), exc_info=True)
        raise


DEFAULT_FLOW_TIMEOUT = 9000 

@flow(
    name="Pipedrive to Database ETL Flow",
    log_prints=True,
    retries=0,
    timeout_seconds=DEFAULT_FLOW_TIMEOUT
)
def main_etl_flow(run_batch_size: int = DEFAULT_BATCH_SIZE):
    """Main ETL orchestration flow."""
    setup_logging(level=logging.DEBUG)  
    flow_log = log.bind(flow_name="main_etl_flow", run_batch_size=run_batch_size)
    flow_log.info("Starting main ETL flow.")

    try:
        # Submit the ETL task
        etl_future = execute_etl_task.submit(batch_size=run_batch_size)

        # Wait for the result
        result = etl_future.result() 

        # --- Post-Load Validation ---
        flow_log.info("Performing post-load validation.")
        status = result.get("status", "error")
        processed = result.get("total_loaded", 0) 
        duration = result.get("duration_seconds", -1)
        peak_mem = result.get("peak_memory_mb", -1)

        if status != "success":
            flow_log.critical("ETL task reported failure.", result_message=result.get("message"))
            raise RuntimeError(f"ETL task failed: {result.get('message', 'Unknown error')}")

        assert processed >= 0, "Processed count cannot be negative."
        if result.get("total_fetched", 0) > 0 and processed == 0:
            flow_log.warning("ETL fetched data but loaded zero records.", fetched=result.get("total_fetched"))
            raise ValueError("ETL fetched data but loaded zero records.")

        sla_duration = 3600
        assert duration >= 0 and duration < sla_duration, f"ETL duration ({duration:.2f}s) exceeded SLA ({sla_duration}s)"

        sla_memory_mb = 4 * 1024
        if peak_mem > 0:
           assert peak_mem < sla_memory_mb, f"ETL peak memory ({peak_mem:.2f}MB) exceeded limit ({sla_memory_mb}MB)"

        flow_log.info(
            "ETL flow completed successfully.",
            processed_records=processed,
            duration_seconds=f"{duration:.2f}",
            peak_memory_mb=f"{peak_mem:.2f}" if peak_mem > 0 else "N/A"
        )
        return result 

    except Exception as e:
        flow_log.critical("Main ETL flow failed.", error=str(e), exc_info=True)
        raise

if __name__ == "__main__":
   main_etl_flow(run_batch_size=500)