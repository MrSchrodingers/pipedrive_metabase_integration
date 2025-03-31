from prefect import flow, task, get_run_logger
from prefect.task_runners import ConcurrentTaskRunner
from infrastructure.db_pool import DBConnectionPool
from prefect.blocks.system import JSON

@task(
    retries=3,
    retry_delay_seconds=30,
    log_prints=True,
    timeout_seconds=3600
)
@task
def execute_etl(batch_size: int = 10000):
    """Task de ETL com gestão completa de recursos"""
    logger = get_run_logger()
    try:
        # Carregar blocos como dicionários
        postgres_block = JSON.load("postgres-pool").value
        redis_block = JSON.load("redis-cache").value
        
        # Criar pool de conexões
        db_pool = DBConnectionPool(
            minconn=5,
            maxconn=20,
            dsn=postgres_block["dsn"]
        )

        from infrastructure.api_clients.pipedrive_api_client import PipedriveAPIClient
        from infrastructure.repository_impl.pipedrive_repository import PipedriveRepository
        from application.services.etl_service import ETLService
        from infrastructure.cache import RedisCache
        

        client = PipedriveAPIClient(
            cache=RedisCache(connection_string=redis_block["connection_string"])
        )
        custom_mapping = client.fetch_deal_fields_mapping()
        repository = PipedriveRepository(db_pool, custom_mapping)
        
        return ETLService(client, repository, batch_size).run_etl()
    
    except Exception as e:
        logger.error("Erro ao carregar blocks: %s", str(e))
        raise

@flow(
    name="pipedrive_metabase_etl",
    task_runner=ConcurrentTaskRunner(),
    log_prints=True,
    retries=1,
    retry_delay_seconds=120,
    timeout_seconds=7200
)
def main_etl_flow():
    """Orquestração principal do fluxo ETL"""
    logger = get_run_logger()
    logger.info("Iniciando fluxo ETL principal")
    
    try:
        future = execute_etl.submit(batch_size=10000)
        result = future.result()
        
        if not result.get("status") == "success":
            logger.critical("Falha no fluxo ETL: %s", result.get("message"))
            raise RuntimeError(result.get("message"))
        
        logger.info(
            "ETL concluído com sucesso: %d registros processados em %.2f segundos",
            result["processed"], 
            result["duration"]
        )
        return result
        
    except Exception as e:
        logger.error("Erro no fluxo principal: %s", str(e))
        raise

if __name__ == "__main__":
    main_etl_flow()