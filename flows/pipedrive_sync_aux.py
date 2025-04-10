from typing import Generator, List, Dict, Optional
import structlog
from prefect import flow, task, get_run_logger

from application.ports.pipedrive_client_port import PipedriveClientPort
from infrastructure.repository_impl.pipedrive_repository import PipedriveRepository
from flows.pipedrive_metabase_etl import initialize_components_no_maps
from infrastructure.monitoring.metrics import (
  push_metrics_to_gateway, records_synced_counter, sync_counter, sync_failure_counter,
)

log = structlog.get_logger(__name__)

# --- Tasks Genéricas de Sincronização ---
@task(name="Sync Pipedrive Entity Task", retries=2, retry_delay_seconds=60)
def sync_entity_task(
    entity_type: str,
    client: PipedriveClientPort,
    repository: PipedriveRepository
    ) -> int:
    """
    Task genérica para buscar dados de uma entidade Pipedrive via stream
    e fazer upsert no repositório.
    """
    logger = get_run_logger()
    logger.info(f"Starting sync task for entity: {entity_type}")
    sync_counter.labels(entity_type=entity_type).inc()
    total_synced = 0
    processed_count = 0
    batch_size = 500
    data_batch: List[Dict] = []
    stream: Optional[Generator[Dict, None, None]] = None
    upsert_method: Optional[callable] = None

    # Determinar o método de stream e upsert baseado no entity_type
    if entity_type == "users":
        stream = client._fetch_paginated_v1_stream_adapter(f"{client.BASE_URL_V1}/users") 
        upsert_method = repository.upsert_users
    elif entity_type == "persons":
        stream = client._fetch_paginated_v2_stream(f"{client.BASE_URL_V2}/persons")
        upsert_method = repository.upsert_persons
    elif entity_type == "stages":
        try:
             all_data = client._fetch_paginated_v2(f"{client.BASE_URL_V2}/stages")
             if all_data and hasattr(repository, 'upsert_stages'):
                  total_synced = repository.upsert_stages(all_data)
                  records_synced_counter.labels(entity_type=entity_type).inc(total_synced)
                  logger.info(f"Synced {entity_type} in one go", count=total_synced)
             return total_synced
        except Exception as e:
             logger.error(f"Failed to sync {entity_type} in one go", error=str(e), exc_info=True)
             sync_failure_counter.labels(entity_type=entity_type).inc()
             raise e
        # stream = client._fetch_paginated_v2_stream(f"{client.BASE_URL_V2}/stages") # Se usar stream
        # upsert_method = repository.upsert_stages # Se usar stream
    elif entity_type == "pipelines":
        # Similar a stages, buscar todos de uma vez
        try:
             all_data = client._fetch_paginated_v2(f"{client.BASE_URL_V2}/pipelines")
             if all_data and hasattr(repository, 'upsert_pipelines'):
                  total_synced = repository.upsert_pipelines(all_data)
                  records_synced_counter.labels(entity_type=entity_type).inc(total_synced)
                  logger.info(f"Synced {entity_type} in one go", count=total_synced)
             return total_synced
        except Exception as e:
             logger.error(f"Failed to sync {entity_type} in one go", error=str(e), exc_info=True)
             sync_failure_counter.labels(entity_type=entity_type).inc()
             raise e
        # stream = client._fetch_paginated_v2_stream(f"{client.BASE_URL_V2}/pipelines") # Se usar stream
        # upsert_method = repository.upsert_pipelines # Se usar stream
    elif entity_type == "organizations":
        stream = client._fetch_paginated_v2_stream(f"{client.BASE_URL_V2}/organizations") 
        upsert_method = repository.upsert_organizations
    else:
        logger.error(f"Unknown entity type for sync task: {entity_type}")
        raise ValueError(f"Unknown entity type: {entity_type}")

    if not stream or not upsert_method:
         logger.error(f"Stream or upsert method not defined for entity: {entity_type}")
         raise NotImplementedError(f"Sync not implemented for {entity_type}")

    try:
        for item in stream:
            data_batch.append(item)
            processed_count += 1
            if len(data_batch) >= batch_size:
                logger.debug(f"Upserting batch of {len(data_batch)} {entity_type}...")
                upserted = upsert_method(data_batch)
                total_synced += upserted
                records_synced_counter.labels(entity_type=entity_type).inc(upserted)
                data_batch = []

        if data_batch:
            logger.debug(f"Upserting final batch of {len(data_batch)} {entity_type}...")
            upserted = upsert_method(data_batch)
            total_synced += upserted
            records_synced_counter.labels(entity_type=entity_type).inc(upserted)

        logger.info(f"Sync task completed for {entity_type}", total_processed=processed_count, total_upserted=total_synced)
        return total_synced

    except Exception as e:
        logger.error(f"Sync task failed for {entity_type}", error=str(e), exc_info=True)
        sync_failure_counter.labels(entity_type=entity_type).inc()
        raise e

# --- Fluxos Específicos de Sincronização ---
@flow(name="Sync Pipedrive Users", log_prints=True)
def sync_pipedrive_users_flow():
    logger = get_run_logger()
    logger.info("Starting Pipedrive Users sync flow...")
    flow_run_id = get_run_logger().extra.get("flow_run_id", "local_sync_users")
    try:
        client, repository, _ = initialize_components_no_maps()
        sync_entity_task(entity_type="users", client=client, repository=repository)
        logger.info("Pipedrive Users sync flow finished successfully.")
    except Exception as e:
         logger.critical(f"Pipedrive Users sync flow failed. Error: {e}", exc_info=True)
    finally:
        push_metrics_to_gateway(job_name="pipedrive_sync_users", grouping_key={'flow_run_id': str(flow_run_id)})


@flow(name="Sync Pipedrive Persons and Orgs", log_prints=True)
def sync_pipedrive_persons_orgs_flow():
    logger = get_run_logger()
    logger.info("Starting Pipedrive Persons & Orgs sync flow...")
    flow_run_id = get_run_logger().extra.get("flow_run_id", "local_sync_persons_orgs")
    try:
        client, repository, _ = initialize_components_no_maps()
        sync_entity_task(entity_type="persons", client=client, repository=repository)
        sync_entity_task(entity_type="organizations", client=client, repository=repository)
        logger.info("Pipedrive Persons & Orgs sync flow finished successfully.")
    except Exception as e:
         logger.critical("Pipedrive Persons & Orgs sync flow failed.", error=str(e), exc_info=True)
    finally:
        push_metrics_to_gateway(job_name="pipedrive_sync_persons_orgs", grouping_key={'flow_run_id': str(flow_run_id)})


@flow(name="Sync Pipedrive Stages and Pipelines", log_prints=True)
def sync_pipedrive_stages_pipelines_flow():
    logger = get_run_logger()
    logger.info("Starting Pipedrive Stages & Pipelines sync flow...")
    flow_run_id = get_run_logger().extra.get("flow_run_id", "local_sync_stages_pipelines")
    try:
        client, repository, _ = initialize_components_no_maps()
        sync_entity_task(entity_type="stages", client=client, repository=repository)
        sync_entity_task(entity_type="pipelines", client=client, repository=repository)
        logger.info("Pipedrive Stages & Pipelines sync flow finished successfully.")
    except Exception as e:
         logger.critical("Pipedrive Stages & Pipelines sync flow failed.", error=str(e), exc_info=True)
    finally:
        push_metrics_to_gateway(job_name="pipedrive_sync_stages_pipelines", grouping_key={'flow_run_id': str(flow_run_id)})
