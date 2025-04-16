from typing import Any, Generator, List, Dict, Optional, Tuple
import structlog
from prefect import flow, task, get_run_logger

from application.ports.pipedrive_client_port import PipedriveClientPort
from infrastructure.api_clients.pipedrive_api_client import PipedriveAPIClient 
from infrastructure.repository_impl.lookup_repositories import (
    UserRepository, PersonRepository, StageRepository, PipelineRepository, OrganizationRepository
)


from flows.pipedrive_metabase_etl import initialize_components

from infrastructure.monitoring.metrics import (
    push_metrics_to_gateway,
    sync_records_upserted_total,
    sync_runs_total,             
    sync_run_failures_total    
)

log = structlog.get_logger(__name__)

# --- Tasks Genéricas de Sincronização ---
@task(name="Sync Pipedrive Entity Task", retries=2, retry_delay_seconds=60, cache_policy=None)
def sync_entity_task(
    entity_type: str,
    client: PipedriveClientPort,
    repository: (UserRepository | PersonRepository | StageRepository | PipelineRepository | OrganizationRepository)
    ) -> int:
    """
    Task genérica para buscar dados de uma entidade Pipedrive via stream
    e fazer upsert no repositório de lookup correspondente.
    """
    logger = get_run_logger()
    logger.info(f"Starting sync task for entity: {entity_type}")
    sync_runs_total.labels(entity_type=entity_type).inc()
    total_synced = 0
    processed_count = 0
    batch_size = 500 
    data_batch: List[Dict] = []
    stream: Optional[Generator[Dict, None, None]] = None
    upsert_method: Optional[callable] = None

    if entity_type == "users" and isinstance(repository, UserRepository):
        stream = client._fetch_paginated_v1_stream_adapter(f"{client.BASE_URL_V1}/users")
        upsert_method = repository.upsert_users
    elif entity_type == "persons" and isinstance(repository, PersonRepository):
        stream = client._fetch_paginated_v2_stream(f"{client.BASE_URL_V2}/persons")
        upsert_method = repository.upsert_persons
    elif entity_type == "stages" and isinstance(repository, StageRepository):
        stream = client._fetch_paginated_v2_stream(f"{client.BASE_URL_V2}/stages")
        upsert_method = repository.upsert_stages
    elif entity_type == "pipelines" and isinstance(repository, PipelineRepository):
        stream = client._fetch_paginated_v2_stream(f"{client.BASE_URL_V2}/pipelines")
        upsert_method = repository.upsert_pipelines
    elif entity_type == "organizations" and isinstance(repository, OrganizationRepository):
        stream = client._fetch_paginated_v2_stream(f"{client.BASE_URL_V2}/organizations")
        upsert_method = repository.upsert_organizations
    else:
        logger.error(f"Unknown entity type or mismatched repository for sync task: {entity_type}, repo_type={type(repository).__name__}")
        raise ValueError(f"Unknown entity type or repository mismatch: {entity_type}")

    if not stream or not upsert_method:
         logger.error(f"Stream or upsert method could not be determined for entity: {entity_type}")
         raise NotImplementedError(f"Sync logic error for {entity_type}")

    upserted_in_last_batch = 0

    try:
        for item in stream:
            data_batch.append(item)
            processed_count += 1
            if len(data_batch) >= batch_size:
                logger.debug(f"Upserting batch of {len(data_batch)} {entity_type}...")
                upserted_in_last_batch = upsert_method(data_batch)
                total_synced += upserted_in_last_batch
                sync_records_upserted_total.labels(entity_type=entity_type).inc(upserted_in_last_batch)
                data_batch = []
                upserted_in_last_batch = 0

        if data_batch:
            logger.debug(f"Upserting final batch of {len(data_batch)} {entity_type}...")
            upserted_in_last_batch = upsert_method(data_batch)
            total_synced += upserted_in_last_batch
            sync_records_upserted_total.labels(entity_type=entity_type).inc(upserted_in_last_batch)

        logger.info(f"Sync task completed for {entity_type}", total_processed=processed_count, total_upserted=total_synced)
        return total_synced

    except Exception as e:
        logger.error(f"Sync task failed for {entity_type}", exc_info=True)
        sync_run_failures_total.labels(entity_type=entity_type).inc()
        raise e

# --- Fluxos Específicos de Sincronização ---

def _initialize_and_get_repo(entity_type: str, logger: structlog.BoundLoggerBase) -> Tuple[PipedriveAPIClient, Any]:
    logger.info(f"Initializing components for {entity_type} sync using main initialize_components...")
    try:
        components: Tuple = initialize_components()

        # Mapeia o entity_type para o índice correto na tupla retornada por initialize_components
        # (client, data_repo, etl_service, user_repo, person_repo, stage_repo, pipeline_repo, org_repo, config_repo)
        # Índices:   0        1          2            3          4            5           6             7           8
        repo_map = {
            "users": 3,
            "persons": 4,
            "stages": 5,
            "pipelines": 6,
            "organizations": 7
        }
        repo_index = repo_map.get(entity_type)

        if repo_index is None:
            raise ValueError(f"Entity type '{entity_type}' not mapped to a known repository index.")

        client = components[0]      
        repository = components[repo_index]

        logger.info(f"Components initialized for {entity_type}. Using repository: {type(repository).__name__}")
        return client, repository
    except Exception as init_err:
        logger.critical(f"Failed to initialize components for {entity_type}", error=str(init_err), exc_info=True)
        raise

@flow(name="Sync Pipedrive Users", log_prints=True)
def sync_pipedrive_users_flow():
    logger = get_run_logger()
    entity_type = "users"
    logger.info(f"Starting Pipedrive {entity_type} sync flow...")
    flow_run_id = getattr(logger, "extra", {}).get("flow_run_id", f"local_sync_{entity_type}")
    try:
        client, repository = _initialize_and_get_repo(entity_type, logger)
        sync_entity_task(entity_type=entity_type, client=client, repository=repository)
        logger.info(f"Pipedrive {entity_type} sync flow finished successfully.")
    except Exception as e:
         logger.critical(f"Pipedrive {entity_type} sync flow failed at the flow level. Check previous logs.")
    finally:
        logger.info(f"Pushing metrics to Pushgateway for {entity_type} sync flow.")
        push_metrics_to_gateway(job_name=f"pipedrive_sync_{entity_type}", grouping_key={'flow_run_id': str(flow_run_id)})


@flow(name="Sync Pipedrive Persons and Orgs", log_prints=True)
def sync_pipedrive_persons_orgs_flow():
    logger = get_run_logger()
    logger.info("Starting Pipedrive Persons & Orgs sync flow...")
    flow_run_id = get_run_logger().extra.get("flow_run_id", "local_sync_persons_orgs")
    success = True 
    try:
        # Persons
        entity_type_person = "persons"
        logger.info(f"--- Starting {entity_type_person} sync part ---")
        try:
            client_p, repo_p = _initialize_and_get_repo(entity_type_person, logger)
            sync_entity_task(entity_type=entity_type_person, client=client_p, repository=repo_p)
            logger.info(f"--- Finished {entity_type_person} sync part successfully ---")
        except Exception as e_p:
            logger.error(f"--- Failed {entity_type_person} sync part ---", error=str(e_p))
            success = False 

        # Organizations
        entity_type_org = "organizations"
        logger.info(f"--- Starting {entity_type_org} sync part ---")
        try:
            client_o, repo_o = _initialize_and_get_repo(entity_type_org, logger)
            sync_entity_task(entity_type=entity_type_org, client=client_o, repository=repo_o)
            logger.info(f"--- Finished {entity_type_org} sync part successfully ---")
        except Exception as e_o:
            logger.error(f"--- Failed {entity_type_org} sync part ---", error=str(e_o))
            success = False 

        if success:
             logger.info("Pipedrive Persons & Orgs sync flow finished successfully.")
        else:
             logger.warning("Pipedrive Persons & Orgs sync flow finished with one or more errors.")

    except Exception as e: 
         logger.critical("Pipedrive Persons & Orgs sync flow failed unexpectedly at flow level.", exc_info=True)
         success = False
    finally:
        logger.info("Pushing metrics to Pushgateway for Persons & Orgs sync flow.")
        push_metrics_to_gateway(job_name="pipedrive_sync_persons_orgs", grouping_key={'flow_run_id': str(flow_run_id)})


@flow(name="Sync Pipedrive Stages and Pipelines", log_prints=True)
def sync_pipedrive_stages_pipelines_flow():
    logger = get_run_logger()
    logger.info("Starting Pipedrive Stages & Pipelines sync flow...")
    flow_run_id = get_run_logger().extra.get("flow_run_id", "local_sync_stages_pipelines")
    success = True 
    try:
        # Stages
        entity_type_stage = "stages"
        logger.info(f"--- Starting {entity_type_stage} sync part ---")
        try:
            client_s, repo_s = _initialize_and_get_repo(entity_type_stage, logger)
            sync_entity_task(entity_type=entity_type_stage, client=client_s, repository=repo_s)
            logger.info(f"--- Finished {entity_type_stage} sync part successfully ---")
        except Exception as e_s:
             logger.error(f"--- Failed {entity_type_stage} sync part ---", error=str(e_s))
             success = False

        # Pipelines
        entity_type_pipe = "pipelines"
        logger.info(f"--- Starting {entity_type_pipe} sync part ---")
        try:
            client_pl, repo_pl = _initialize_and_get_repo(entity_type_pipe, logger)
            sync_entity_task(entity_type=entity_type_pipe, client=client_pl, repository=repo_pl)
            logger.info(f"--- Finished {entity_type_pipe} sync part successfully ---")
        except Exception as e_pl:
             logger.error(f"--- Failed {entity_type_pipe} sync part ---", error=str(e_pl))
             success = False

        if success:
            logger.info("Pipedrive Stages & Pipelines sync flow finished successfully.")
        else:
            logger.warning("Pipedrive Stages & Pipelines sync flow finished with one or more errors.")

    except Exception as e: 
         logger.critical("Pipedrive Stages & Pipelines sync flow failed unexpectedly at flow level.", exc_info=True)
         success = False
    finally:
        logger.info("Pushing metrics to Pushgateway for Stages & Pipelines sync flow.")
        push_metrics_to_gateway(job_name="pipedrive_sync_stages_pipelines", grouping_key={'flow_run_id': str(flow_run_id)})