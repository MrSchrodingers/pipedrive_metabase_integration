import time
from typing import Any, Dict, List, Tuple
import pandas as pd
import structlog
from prefect import flow, get_run_logger, task, context
from prefect.blocks.system import JSON
import logging

from application.services.etl_service import ETLService
from flows.utils.flows_utils import calculate_optimal_batch_size, get_optimal_batch_size, update_dynamic_batch_config, validate_loaded_data
from infrastructure.api_clients.pipedrive_api_client import PipedriveAPIClient
from infrastructure.cache import RedisCache
from infrastructure.db_pool import DBConnectionPool
from infrastructure.logging_config import setup_logging
from infrastructure.repository_impl.pipedrive_repository import PipedriveRepository
from infrastructure.monitoring.metrics import (
    push_metrics_to_gateway,
    etl_counter,
    etl_failure_counter,
    memory_usage_gauge,
    batch_size_gauge,
    backfill_deals_remaining_gauge,
    batch_experiment_counter
)

log = structlog.get_logger() 

# --- Constantes e Configurações ---
DEFAULT_BATCH_SIZE = 1000
DEFAULT_MAIN_FLOW_TIMEOUT = 9000        # Timeout para o fluxo principal
DEFAULT_BACKFILL_FLOW_TIMEOUT = 10800   # Timeout maior para backfill
BACKFILL_BATCH_SIZE = 1000              # Quantos deals buscar do DB por vez
BACKFILL_DAILY_LIMIT = 10000            # Limite diário sugerido
DEFAULT_TASK_RETRIES = 3
DEFAULT_TASK_RETRY_DELAY = 60



@task(name="Initialize ETL Components", retries=2, retry_delay_seconds=30)
def initialize_components() -> Tuple[PipedriveAPIClient, PipedriveRepository, ETLService]:
    """Inicializa e retorna os componentes principais."""
    task_log = get_run_logger()
    task_log.info("Initializing ETL components...")

    postgres_config = JSON.load("postgres-pool").value
    redis_config = JSON.load("redis-cache").value

    db_pool = DBConnectionPool(
        minconn=postgres_config.get("minconn", 1), 
        maxconn=postgres_config.get("maxconn", 5),
        dsn=postgres_config["dsn"]
    )
    redis_cache = RedisCache(connection_string=redis_config["connection_string"])
    pipedrive_client = PipedriveAPIClient(cache=redis_cache)

    try:
        all_stages = pipedrive_client.fetch_all_stages_details()
        if not all_stages:
             task_log.warning("Fetched stage details list is empty!")
    except Exception as stage_err:
        task_log.error("Failed to fetch stage details during initialization.", error=str(stage_err))
        raise stage_err

    try:
        custom_mapping = pipedrive_client.fetch_deal_fields_mapping()
    except Exception as mapping_err:
        task_log.error("Failed to fetch custom field mapping.", error=str(mapping_err))
        raise mapping_err

    repository = PipedriveRepository(
        db_pool=db_pool,
        custom_field_api_mapping=custom_mapping,
        all_stages_details=all_stages
    )
    etl_service = ETLService(
        client=pipedrive_client,
        repository=repository,
    )
    task_log.info("ETL components initialized.")
    return pipedrive_client, repository, etl_service


@flow(
    name="Pipedrive to Database ETL Flow (Main Sync)",
    log_prints=True,
    timeout_seconds=DEFAULT_MAIN_FLOW_TIMEOUT 
)
def main_etl_flow(run_batch_size: int = DEFAULT_BATCH_SIZE):
    """Main ETL orchestration flow for current data sync."""
    setup_logging(level=logging.INFO)
    base_flow_log = get_run_logger() 
    flow_run_ctx = context.get_run_context().flow_run
    flow_run_id = flow_run_ctx.id if flow_run_ctx else "local"

    flow_log = base_flow_log
    flow_run_name = flow_run_ctx.name if flow_run_ctx else "Unknown Run"
    flow_log.info(f"Starting flow run '{flow_run_name}'...")

    flow_log.info(f"Starting main ETL flow with batch size: {run_batch_size}")
    flow_type = "sync"
    etl_counter.labels(flow_type=flow_type).inc()
    start_time = time.time()
    result = {}

    try:
        # 1. Inicializa componentes (incluindo busca de stages/fields)
        client, repository, etl_service = initialize_components()

        # 2. Executa o ETL principal (run_etl agora foca na sincronização atual)
        optimal_batch = get_optimal_batch_size(repository)
        base_flow_log.info(f"Using optimal batch size from config: {optimal_batch}")
        etl_service.process_batch_size = optimal_batch
        
        result = etl_service.run_etl(flow_type="sync")

        # --- Validação e Métricas ---
        flow_log.info("Performing post-load validation.")
        status = result.get("status", "error")
        processed = result.get("total_loaded", 0) 
        duration = result.get("duration_seconds", -1)
        peak_mem = result.get("peak_memory_mb", -1)

        if status != "success":
            etl_failure_counter.labels(flow_type=flow_type).inc() 
            message = result.get("message", "Unknown error")
            flow_log.critical(f"ETL task reported failure. Message: {message}")
            raise RuntimeError(f"ETL task failed: {result.get('message', 'Unknown error')}")


        assert processed >= 0, "Processed count cannot be negative."
        if result.get("total_fetched", 0) > 0 and processed == 0:
            flow_log.warning("ETL fetched data but loaded zero records.", fetched=result.get("total_fetched"))
            raise ValueError("ETL fetched data but loaded zero records.")

        sla_duration = 3600
        if duration < 0 or duration >= sla_duration:
             flow_log.warning(f"ETL duration ({duration:.2f}s) outside expected range (0-{sla_duration}s)")

        sla_memory_mb = 4 * 1024
        if peak_mem > 0 and peak_mem >= sla_memory_mb:
             flow_log.warning(f"ETL peak memory ({peak_mem:.2f}MB) approached or exceeded limit ({sla_memory_mb}MB)")

        peak_mem_str = f"{peak_mem:.2f}" if peak_mem > 0 else "N/A"
        flow_log.info(
            f"ETL flow completed successfully. "
            f"Processed Records: {processed}, "
            f"Duration: {duration:.2f}s, "
            f"Peak Memory: {peak_mem_str}MB"
        )
        
        if peak_mem > 0:
             memory_usage_gauge.labels(flow_type=flow_type).set(peak_mem)
        return result 

    except Exception as e:
        if result.get("status") != "error": 
            etl_failure_counter.labels(flow_type=flow_type).inc()
        flow_log.critical(f"Main ETL flow failed critically: {str(e)}", exc_info=True)
        raise 

    finally:
        flow_log.info("Pushing metrics to Pushgateway for main sync flow.")
        push_metrics_to_gateway(job_name="pipedrive_sync_job", grouping_key={'flow_run_id': str(flow_run_id)})



    
@task(name="Get Deals for Backfill Task", retries=1)
def get_deals_for_backfill_task(repository: PipedriveRepository, limit: int) -> List[str]:
    """Busca IDs de deals que precisam de backfill."""
    logger = get_run_logger()
    logger.info(f"Fetching up to {limit} deal IDs for history backfill.")
    ids = repository.get_deals_needing_history_backfill(limit=limit)
    logger.info(f"Found {len(ids)} deals for this backfill batch.")
    return ids

@task(name="Get Backfill Remaining Count Task", retries=1)
def get_initial_backfill_count_task(repository: PipedriveRepository) -> int:
    """Busca a contagem inicial de deals que precisam de backfill."""
    logger = get_run_logger()
    logger.info("Counting total deals needing history backfill.")
    count = repository.count_deals_needing_backfill()
    if count >= 0:
         logger.info(f"Estimated {count} deals remaining for backfill.")
         backfill_deals_remaining_gauge.set(count) 
    else:
         logger.warning("Failed to get backfill remaining count.")
         backfill_deals_remaining_gauge.set(-1) 
    return count

@task(name="Run Backfill Batch Task", retries=DEFAULT_TASK_RETRIES, retry_delay_seconds=DEFAULT_TASK_RETRY_DELAY, log_prints=True)
def run_backfill_batch_task(etl_service: ETLService, deal_ids: List[str]) -> Dict[str, Any]:
    """Executa o backfill para um lote de IDs."""
    logger = get_run_logger()
    flow_type="backfill"
    if not deal_ids:
        logger.info("No deals in this batch to backfill.")
        return {"status": "skipped", "processed_deals": 0}
    batch_size_gauge.labels(flow_type=flow_type).set(len(deal_ids))
    logger.info(f"Running backfill for {len(deal_ids)} deals.")
    result = etl_service.run_retroactive_backfill(deal_ids)
    logger.info("Backfill batch finished.", **result)
    return result

@flow(
    name="Batch Size Experiment Flow - Enhanced",
    log_prints=True,
    timeout_seconds=10800  # 3 horas para experimentos longos
)
def batch_size_experiment_flow(
    batch_sizes: List[int] = [300, 500, 750, 1000, 1500, 2000],
    test_data_size: int = 10000  # Quantidade de dados reais a serem usados
):
    """Fluxo aprimorado para experimentos de tamanho de batch com análise automática e validação."""
    setup_logging(level=logging.INFO)
    flow_log = get_run_logger()
    flow_run_ctx = context.get_run_context().flow_run
    flow_run_id = flow_run_ctx.id if flow_run_ctx else "local"

    try:
        # 1. Buscar dados reais para o teste
        client, repository, etl_service = initialize_components()
        flow_log.info(f"Fetching {test_data_size} recent deals for testing...")
        test_data = list(client.fetch_all_deals_stream(items_limit=test_data_size))
        
        if not test_data:
            raise ValueError("No test data available for experiment")

        # 2. Executar experimentos
        results = []
        for size in batch_sizes:
            flow_log.info(f"Starting experiment with batch size: {size}")
            
            # Configurar métricas
            metrics_labels = {
                "experiment": "batch_size", 
                "batch_size": str(size),
                "flow_run_id": str(flow_run_id)
            }
            
            # Executar ETL e coletar métricas
            with batch_experiment_counter.labels(**metrics_labels).count_exceptions():
                start_time = time.monotonic()
                
                # Processar dados de teste
                result = etl_service.run_etl_with_data(test_data, batch_size=size, flow_type="experiment")
                
                duration = time.monotonic() - start_time
                records_processed = result.get("total_loaded", 0)
                
                # Coletar métricas
                batch_metrics = {
                    'batch_size': size,
                    'duration': duration,
                    'throughput': records_processed / duration if duration > 0 else 0,
                    'memory_peak': result.get("peak_memory_mb", 0),
                    'success_rate': result.get("success_rate", 0),
                    'data_quality_issues': result.get("data_quality_issues", 0)
                }
                
                # Publicar métricas em tempo real
                push_metrics_to_gateway(
                    job_name="batch_experiment",
                    grouping_key=metrics_labels
                )
                
                # Validação dos dados
                validation_result = validate_loaded_data(
                    repository=repository,
                    source_data=test_data,
                    batch_size=size
                )
                
                batch_metrics.update(validation_result)
                results.append(batch_metrics)

                flow_log.info(
                    "Batch experiment completed",
                    extra={
                        "batch_size": size,
                        "duration": duration,
                        "throughput": records_processed / duration if duration > 0 else 0,
                        "memory_peak": result.get("peak_memory_mb", 0),
                        "success_rate": result.get("success_rate", 0),
                        "data_quality_issues": result.get("data_quality_issues", 0),
                        "validation_result": validation_result
                    }
                )
                
        # 3. Análise Automática
        optimal_size = calculate_optimal_batch_size(results)
        flow_log.info(
            "Optimal batch size determined",
            extra={
                "optimal_batch_size": optimal_size,
                "analysis_metrics": results
            }
        )

        # 4. Persistir resultados
        df = pd.DataFrame(results)
        df.to_csv('batch_metrics.csv', index=False)
        
        # 5. Atualizar configuração dinâmica
        update_dynamic_batch_config(repository, optimal_size)
        
        return {
            "status": "completed",
            "optimal_batch_size": optimal_size,
            "detailed_results": results
        }

    except Exception as e:
        flow_log.error("Batch experiment failed", exc_info=str(e))
        raise


@flow(
    name="Pipedrive Stage History Backfill Flow",
    log_prints=True,
    timeout_seconds=DEFAULT_BACKFILL_FLOW_TIMEOUT
)
def backfill_stage_history_flow(daily_deal_limit: int = BACKFILL_DAILY_LIMIT, db_batch_size: int = BACKFILL_BATCH_SIZE):
    """Orquestra o backfill do histórico de stages em lotes."""
    setup_logging(level=logging.INFO)
    base_flow_log = get_run_logger()
    flow_run_ctx = context.get_run_context().flow_run
    flow_run_id = flow_run_ctx.id if flow_run_ctx else "local"

    flow_log = base_flow_log
    flow_log.info(f"Starting {flow_run_ctx.flow.name} flow...")

    flow_log.info(f"Starting stage history backfill flow. Daily limit: {daily_deal_limit}, DB batch size: {db_batch_size}")
    flow_type = "backfill"
    etl_counter.labels(flow_type=flow_type).inc()
    start_time = time.time()

    total_processed_today = 0
    total_api_errors = 0
    total_processing_errors = 0
    all_batch_results = []
    final_status = "completed"
    initial_count = -1
    backfill_completed_successfully = False 
    result_payload = {}

    try:
        client, repository, etl_service = initialize_components()

        initial_count = get_initial_backfill_count_task(repository)

        while total_processed_today < daily_deal_limit:
            remaining_limit = daily_deal_limit - total_processed_today
            current_batch_limit = min(db_batch_size, remaining_limit)
            if current_batch_limit <= 0:
                 flow_log.info("Daily limit reached.")
                 break

            flow_log.info(f"Attempting to fetch next batch of deals (limit: {current_batch_limit}).")
            deal_ids_batch = get_deals_for_backfill_task(repository, limit=current_batch_limit)

            if not deal_ids_batch:
                flow_log.info("No more deals found needing backfill.")
                backfill_deals_remaining_gauge.set(0)
                backfill_completed_successfully = (final_status == "completed")
                break 

            batch_result = run_backfill_batch_task(etl_service, deal_ids_batch)
            all_batch_results.append(batch_result)

            processed_in_batch = batch_result.get("processed_deals", 0)
            api_errors_in_batch = batch_result.get("api_errors", 0)
            proc_errors_in_batch = batch_result.get("processing_errors", 0)

            total_processed_today += processed_in_batch
            total_api_errors += api_errors_in_batch
            total_processing_errors += proc_errors_in_batch

            if batch_result.get("status") != "skipped" and batch_result.get("status") != "success":
                 final_status = "completed_with_errors"

            if initial_count >= 0:
                 current_remaining = max(0, initial_count - total_processed_today)
                 backfill_deals_remaining_gauge.set(current_remaining)

            flow_log.info(f"Backfill batch completed. Processed so far today: {total_processed_today}/{daily_deal_limit}")

            time.sleep(5)

        flow_log.info("Backfill flow finished for today.",
                       total_processed=total_processed_today,
                       total_api_errors=total_api_errors,
                       total_processing_errors=total_processing_errors,
                       final_status=final_status)

        final_remaining_count = -1
        try:
            final_remaining_count = repository.count_deals_needing_backfill()
            if final_remaining_count >= 0:
                 backfill_deals_remaining_gauge.set(final_remaining_count)
                 if final_remaining_count == 0 and final_status == "completed":
                     backfill_completed_successfully = True
            else:
                 backfill_deals_remaining_gauge.set(-1)
                 final_status = "completed_with_errors" 
        except Exception as count_err:
            flow_log.error("Failed to get final remaining count", error=str(count_err))
            backfill_deals_remaining_gauge.set(-1)
            final_status = "completed_with_errors"
            
        if final_status != "completed":
             etl_failure_counter.labels(flow_type=flow_type).inc()

        try:
             final_remaining = repository.count_deals_needing_backfill()
             if final_remaining >= 0:
                  backfill_deals_remaining_gauge.set(final_remaining) 
             else:
                  backfill_deals_remaining_gauge.set(-1)
        except Exception:
             flow_log.warning("Could not get final remaining count for backfill gauge.")
             backfill_deals_remaining_gauge.set(-1)
             
        result_payload = {
            "status": final_status,
            "total_processed_deals": total_processed_today,
            "total_api_errors": total_api_errors,
            "total_processing_errors": total_processing_errors,
            "batch_results": all_batch_results,
            "backfill_complete": backfill_completed_successfully, 
            "estimated_remaining": final_remaining_count 
        }
        flow_log.info("Final backfill run result", **result_payload)
        return result_payload

    except Exception as e:
        etl_failure_counter.labels(flow_type=flow_type).inc()
        flow_log.critical("Backfill flow failed critically.", error=str(e), exc_info=True)
        final_status = "failed"
        raise

    finally:
        flow_log.info("Pushing metrics to Pushgateway for backfill flow.")
        push_metrics_to_gateway(job_name="pipedrive_backfill_job", grouping_key={'flow_run_id': str(flow_run_id)})
