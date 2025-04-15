import datetime
import time
from typing import Any, Dict, List, Tuple
import pandas as pd
from prefect import flow, get_run_logger, task, context
from prefect.blocks.system import JSON

import logging

from application.services.etl_service import ETLService
from infrastructure.api_clients.pipedrive_api_client import PipedriveAPIClient
from infrastructure.cache import RedisCache
from infrastructure.db.db_pool import DBConnectionPool
from infrastructure.db.schema_manager import SchemaManager
from infrastructure.repository_impl.lookup_repositories import (
    UserRepository, PersonRepository, StageRepository, PipelineRepository, OrganizationRepository
)
from infrastructure.repository_impl.config_repository import ConfigRepository
from application.mappers import deal_mapper

from infrastructure.monitoring.metrics import (
    push_metrics_to_gateway,
    etl_counter,
    etl_failure_counter,
    backfill_deals_remaining_gauge,
    etl_heartbeat,
)
from infrastructure.repository_impl.pipedrive_repository import PipedriveDataRepository

DEFAULT_MAIN_FLOW_TIMEOUT = 9000
DEFAULT_BACKFILL_FLOW_TIMEOUT = 10800
BACKFILL_DAILY_LIMIT = 2000
BACKFILL_DB_BATCH_SIZE = 1000 
DEFAULT_TASK_RETRIES = 3
DEFAULT_TASK_RETRY_DELAY = 60
DEFAULT_OPTIMAL_BATCH_SIZE = 1000 

@task(name="Initialize ETL Components", retries=2, retry_delay_seconds=30)
def initialize_components() -> Tuple[
    PipedriveAPIClient, PipedriveDataRepository, ETLService,
    UserRepository, PersonRepository, StageRepository, PipelineRepository, OrganizationRepository,
    ConfigRepository 
]:
    task_log = get_run_logger()
    task_log.info("Initializing ETL components...")

    try:
        postgres_config = JSON.load("postgres-pool").value
        redis_config = JSON.load("redis-cache").value
    except Exception as block_err:
        task_log.error("Failed to load Prefect Blocks (JSON/Secret)", error=str(block_err))
        raise RuntimeError("Missing required Prefect Blocks (postgres-pool, redis-cache)") from block_err

    db_pool = DBConnectionPool(
        minconn=postgres_config.get("minconn", 1),
        maxconn=postgres_config.get("maxconn", 10),
        dsn=postgres_config["dsn"]
    )
    redis_cache = RedisCache(connection_string=redis_config["connection_string"])
    pipedrive_client = PipedriveAPIClient(cache=redis_cache)

    schema_manager = SchemaManager(db_pool=db_pool)

    all_stages = pipedrive_client.fetch_all_stages_details()
    if not all_stages:
         task_log.warning("Fetched stage details list is empty during initialization!")

    data_repo = PipedriveDataRepository(db_pool, schema_manager, all_stages)
    user_repo = UserRepository(db_pool, schema_manager)
    person_repo = PersonRepository(db_pool, schema_manager)
    stage_repo = StageRepository(db_pool, schema_manager)
    pipeline_repo = PipelineRepository(db_pool, schema_manager)
    org_repo = OrganizationRepository(db_pool, schema_manager)
    config_repo = ConfigRepository(db_pool, schema_manager)

    try:
        task_log.info("Initializing database schemas...")
        data_repo.initialize_schema()
        user_repo.initialize_schema()
        person_repo.initialize_schema()
        stage_repo.initialize_schema()
        pipeline_repo.initialize_schema()
        org_repo.initialize_schema()
        config_repo.initialize_schema()
        task_log.info("Database schemas initialized.")
    except Exception as schema_err:
         task_log.error("Failed during schema initialization", error=str(schema_err), exc_info=True)
         raise RuntimeError("Database schema initialization failed") from schema_err

    optimal_batch_size = DEFAULT_OPTIMAL_BATCH_SIZE 
    try:
        config_val = config_repo.get_configuration("optimal_batch_size")
        if config_val and isinstance(config_val.get("value"), int) and config_val["value"] > 0:
             optimal_batch_size = config_val["value"]
             task_log.info(f"Using optimal batch size from config: {optimal_batch_size}")
        else:
             task_log.warning(f"Optimal batch size not found/invalid in config, using default: {optimal_batch_size}")
    except Exception as config_err:
         task_log.warning(f"Failed to read optimal batch size config, using default: {optimal_batch_size}", error=str(config_err))


    etl_service = ETLService(
        client=pipedrive_client,
        data_repository=data_repo,
        user_repository=user_repo,
        person_repository=person_repo,
        stage_repository=stage_repo,
        pipeline_repository=pipeline_repo,
        org_repository=org_repo,
        mapper_module=deal_mapper,
        batch_size=optimal_batch_size
    )

    task_log.info("ETL components initialized successfully.")
    return pipedrive_client, data_repo, etl_service, user_repo, person_repo, stage_repo, pipeline_repo, org_repo, config_repo

@flow(
    name="Pipedrive to Database ETL Flow (Main Sync)",
    log_prints=True,
    timeout_seconds=DEFAULT_MAIN_FLOW_TIMEOUT
)
def main_etl_flow():
    flow_log = get_run_logger()
    flow_run_ctx = context.get_run_context().flow_run
    flow_run_id = str(flow_run_ctx.id) if flow_run_ctx else "local_main_sync"
    flow_type = "sync"
    flow_log.info(f"Starting flow run '{flow_run_ctx.name if flow_run_ctx else 'main_etl_flow'}'...", extra={"flow_run_id": flow_run_id})

    result = {}
    try:
        _, _, etl_service, _, _, _, _, _, _ = initialize_components()
        result = etl_service.run_etl(flow_type=flow_type)

        if result.get("status", "error") not in ("success", "success_no_new_data"):
            message = result.get("message", "Unknown error in ETL service")
            flow_log.error(f"ETL Service reported failure/errors: {message}", etl_result=result)
            raise RuntimeError(f"ETL task failed: {message}")

        flow_log.info("Main ETL flow completed.", status=result.get("status"))

    except Exception as e:
        flow_log.critical(f"Main ETL flow failed critically: {str(e)}", exc_info=True)
        if not result or result.get("status", "error") == "error":
             etl_failure_counter.labels(flow_type=flow_type).inc()
        raise

    finally:
        etl_heartbeat.labels(flow_type=flow_type).set_to_current_time()
        flow_log.info("Pushing metrics to Pushgateway for main sync flow.")
        push_metrics_to_gateway(job_name="pipedrive_sync_job", grouping_key={'flow_run_id': flow_run_id})

@task(name="Get Deals for Backfill Task", retries=1)
def get_deals_for_backfill_task(data_repo: PipedriveDataRepository, limit: int) -> List[str]:
    task_log = get_run_logger()
    task_log.info(f"Fetching up to {limit} deal IDs for history backfill.")
    ids = data_repo.get_deals_needing_history_backfill(limit=limit)
    task_log.info(f"Found {len(ids)} deals for this backfill batch.")
    return ids

@task(name="Get Backfill Remaining Count Task", retries=1)
def get_initial_backfill_count_task(data_repo: PipedriveDataRepository) -> int:
    task_log = get_run_logger()
    task_log.info("Counting total deals needing history backfill.")
    count = data_repo.count_deals_needing_backfill()
    if count >= 0:
        task_log.info(f"Estimated {count} deals remaining for backfill.")
        backfill_deals_remaining_gauge.set(count)
    else:
        task_log.warning("Failed to get backfill remaining count.")
        backfill_deals_remaining_gauge.set(-1)
    return count

@task(
    name="Run Backfill Batch Task",
    retries=DEFAULT_TASK_RETRIES,
    retry_delay_seconds=DEFAULT_TASK_RETRY_DELAY,
    log_prints=True,
)
def run_backfill_batch_task(etl_service: ETLService, deal_ids: List[str]) -> Dict[str, Any]:
    task_log = get_run_logger()
    if not deal_ids:
        task_log.info("No deals in this batch to backfill.")
        return {"status": "skipped", "processed_deals": 0}

    task_log.info(f"Running backfill for {len(deal_ids)} deals.")
    result = etl_service.run_retroactive_backfill(deal_ids)
    task_log.info("Backfill batch finished.", extra=result)
    return result

@flow(
    name="Pipedrive Stage History Backfill Flow",
    log_prints=True,
    timeout_seconds=DEFAULT_BACKFILL_FLOW_TIMEOUT
)
def backfill_stage_history_flow(
    daily_deal_limit: int = BACKFILL_DAILY_LIMIT,
    db_batch_size: int = BACKFILL_DB_BATCH_SIZE 
):
    flow_log = get_run_logger()
    flow_run_ctx = context.get_run_context().flow_run
    flow_run_id = str(flow_run_ctx.id) if flow_run_ctx else "local_backfill"
    flow_type = "backfill"
    flow_log.info(f"Starting {flow_run_ctx.name if flow_run_ctx else 'backfill_flow'}", extra={"flow_run_id": flow_run_id})
    flow_log.info(f"Daily limit: {daily_deal_limit}, DB fetch batch size: {db_batch_size}")
    etl_counter.labels(flow_type=flow_type).inc()

    total_processed_today = 0
    final_status = "completed"
    backfill_completed_successfully = False
    result_payload = {}

    try:
        _, data_repo, etl_service, _, _, _, _, _, _ = initialize_components()
        initial_count = get_initial_backfill_count_task(data_repo=data_repo)

        while total_processed_today < daily_deal_limit:
            remaining_limit = daily_deal_limit - total_processed_today
            current_batch_limit = min(db_batch_size, remaining_limit)
            if current_batch_limit <= 0:
                flow_log.info("Daily limit reached.")
                break

            flow_log.info(f"Attempting to fetch next batch of deals (limit: {current_batch_limit}).")
            deal_ids_batch = get_deals_for_backfill_task(data_repo=data_repo, limit=current_batch_limit)

            if not deal_ids_batch:
                flow_log.info("No more deals found needing backfill.")
                backfill_deals_remaining_gauge.set(0)
                backfill_completed_successfully = True
                break

            batch_result = run_backfill_batch_task(etl_service=etl_service, deal_ids=deal_ids_batch)
            processed_in_batch = batch_result.get("processed_deals", 0)
            total_processed_today += processed_in_batch

            if batch_result.get("status") != "success" and batch_result.get("status") != "skipped":
                 final_status = "completed_with_errors"

            if initial_count >= 0:
                 current_remaining = max(0, initial_count - total_processed_today)
                 backfill_deals_remaining_gauge.set(current_remaining)

            flow_log.info(f"Backfill batch completed. Processed so far today: {total_processed_today}/{daily_deal_limit}")
            time.sleep(2) 

        flow_log.info("Backfill flow processing loop finished for today.", final_status=final_status, total_processed=total_processed_today)

        final_remaining_count = data_repo.count_deals_needing_backfill()
        if final_remaining_count == 0 and final_status == "completed":
             backfill_completed_successfully = True
        elif final_remaining_count < 0:
             final_status = "completed_with_errors"

        result_payload = {
            "status": final_status,
            "total_processed_deals": total_processed_today,
            "backfill_complete": backfill_completed_successfully,
            "estimated_remaining": final_remaining_count
        }
        flow_log.info("Final backfill run result", **result_payload)

        if final_status != "completed" and not backfill_completed_successfully:
             etl_failure_counter.labels(flow_type=flow_type).inc()

    except Exception as e:
        etl_failure_counter.labels(flow_type=flow_type).inc()
        flow_log.critical("Backfill flow failed critically.", exc_info=True)
        final_status = "failed"
        result_payload = {"status": final_status, "error": str(e)}
        raise

    finally:
        etl_heartbeat.labels(flow_type=flow_type).set_to_current_time()
        flow_log.info("Pushing metrics to Pushgateway for backfill flow.")
        push_metrics_to_gateway(job_name="pipedrive_backfill_job", grouping_key={'flow_run_id': flow_run_id})

@flow(
    name="Batch Size Experiment Flow",
    log_prints=True,
    timeout_seconds=10800
)
def batch_size_experiment_flow(
    batch_sizes: List[int] = [300, 500, 750, 1000, 1500, 2000],
    test_data_size: int = 5000 
):
    flow_log = get_run_logger()
    flow_run_ctx = context.get_run_context().flow_run
    flow_run_id = str(flow_run_ctx.id) if flow_run_ctx else "local_batch_experiment"
    flow_log.info("Starting batch size experiment flow.", extra={"flow_run_id": flow_run_id})

    results = []
    optimal_size = DEFAULT_OPTIMAL_BATCH_SIZE

    try:
        client, data_repo, user_repo, person_repo, stage_repo, pipeline_repo, org_repo, config_repo = initialize_components()

        flow_log.info(f"Fetching up to {test_data_size} recent deals for testing...")
        test_data = list(client.fetch_all_deals_stream(items_limit=test_data_size))
        if not test_data:
            raise ValueError(f"No test data ({test_data_size} deals) could be fetched.")
        flow_log.info(f"Fetched {len(test_data)} deals for experiment.")

        for size in batch_sizes:
            batch_log = flow_log.bind(batch_size=size)
            batch_log.info(f"Starting experiment run with batch size: {size}")
            experiment_etl_service = ETLService(
                 client, data_repo, user_repo, person_repo, stage_repo, pipeline_repo, org_repo,
                 deal_mapper, batch_size=size
            )
            run_result = {}
            try:
                run_result = experiment_etl_service.run_etl_with_data(
                    data=test_data,
                    batch_size=size, 
                    flow_type="experiment"
                )
                metrics = {
                    'batch_size': size,
                    'status': run_result.get("status", "error"),
                    'duration': run_result.get("duration_seconds"),
                    'total_loaded': run_result.get("total_loaded"),
                    'total_failed': (run_result.get("total_schema_failed", 0) +
                                     run_result.get("total_domain_failed", 0) +
                                     run_result.get("total_load_failed", 0)),
                    'memory_peak': run_result.get("peak_memory_mb"),
                    'data_quality_issues': (run_result.get("total_schema_failed", 0) +
                                            run_result.get("total_domain_failed", 0))
                }
                results.append(metrics)
                batch_log.info("Experiment run completed.", extra=metrics)

            except Exception as exp_err:
                batch_log.error("Experiment run failed", error=str(exp_err), exc_info=True)
                results.append({
                     'batch_size': size, 'status': 'critical_error', 'duration': None,
                     'total_loaded': 0, 'total_failed': len(test_data), 'memory_peak': None,
                     'data_quality_issues': len(test_data)
                })

            time.sleep(5)

        # --- Calculate Optimal Size ---
        if not results:
             flow_log.warning("No results collected from experiments.")
        else:
            df = pd.DataFrame(results)
            flow_log.info("Experiment Results Summary:\n" + df.to_string())
            valid_df = df[
                 (df['status'] == 'success') &
                 df['duration'].notna() & (df['duration'] > 0) &
                 df['memory_peak'].notna() & (df['memory_peak'] >= 0) &
                 df['data_quality_issues'].notna()
            ].copy()

            if valid_df.empty:
                 flow_log.error("No valid successful runs found to calculate optimal batch size.")
            else:
                 # Score: Higher throughput (loaded/duration) is better, lower memory is better
                 valid_df['throughput'] = valid_df['total_loaded'] / valid_df['duration']
                 max_throughput = valid_df['throughput'].max()
                 max_memory = valid_df['memory_peak'].max()
                 valid_df['norm_throughput'] = valid_df['throughput'] / max_throughput if max_throughput > 0 else 0
                 valid_df['norm_memory'] = valid_df['memory_peak'] / max_memory if max_memory > 0 else 0
                 valid_df['score'] = (0.6 * valid_df['norm_throughput']) + (0.4 * (1 - valid_df['norm_memory']))

                 best_run = valid_df.loc[valid_df['score'].idxmax()]
                 optimal_size = int(best_run['batch_size'])
                 flow_log.info(f"Optimal batch size calculated: {optimal_size} (Score: {best_run['score']:.3f})")
                 flow_log.info("Best Run Details:\n" + best_run.to_string())

                 # --- Save Optimal Size to Config ---
                 try:
                     config_value = {'value': optimal_size, 'source': 'experiment', 'run_id': flow_run_id, 'calculation_time': datetime.now(datetime.timezone.utc).isoformat()}
                     config_repo.save_configuration(key="optimal_batch_size", value=config_value)
                     flow_log.info(f"Saved optimal batch size {optimal_size} to configuration.")
                 except Exception as save_err:
                      flow_log.error("Failed to save optimal batch size configuration", error=str(save_err), exc_info=True)

    except Exception as e:
        flow_log.critical("Batch size experiment flow failed critically.", exc_info=True)
        raise
    finally:
        flow_log.info("Pushing metrics to Pushgateway for batch experiment flow.")
        push_metrics_to_gateway(job_name="batch_experiment", grouping_key={'flow_run_id': flow_run_id})

    return {"status": "completed", "optimal_batch_size_calculated": optimal_size, "results": results}