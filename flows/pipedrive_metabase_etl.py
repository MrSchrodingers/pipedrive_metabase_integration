from prefect import flow, task

@task(retries=3, retry_delay_seconds=10)
def run_etl_task():
    from infrastructure.db_pool import DBConnectionPool
    from infrastructure.api_clients.pipedrive_api_client import PipedriveAPIClient
    from infrastructure.repository_impl.pipedrive_repository import PipedriveRepository
    from application.services.etl_service import ETLService

    pool = DBConnectionPool(minconn=1, maxconn=10)
    client = PipedriveAPIClient()
    custom_field_mapping = client.fetch_deal_fields_mapping()
    repository = PipedriveRepository(pool, custom_field_mapping)
    
    etl_service = ETLService(client, repository)
    result = etl_service.run_etl() 
    return result

@flow(name="pipedrive_metabase_etl")
def main_flow():
    result = run_etl_task()
    print("ETL executado com sucesso. Resultado:", result)

if __name__ == "__main__":
    main_flow()
