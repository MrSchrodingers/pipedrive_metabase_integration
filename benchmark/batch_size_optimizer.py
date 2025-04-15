import pandas as pd
from application.services.etl_service import ETLService
from infrastructure.api_clients.pipedrive_api_client import PipedriveAPIClient
from infrastructure.cache import RedisCache
from infrastructure.db.db_pool import DBConnectionPool
from infrastructure.repository_impl.pipedrive_repository import PipedriveRepository
from infrastructure.config.settings import settings

def run_batch_size_experiment():
    # Configuração de ambiente
    client = PipedriveAPIClient(cache=RedisCache(settings.REDIS_URL))
    repository = PipedriveRepository(
        db_pool=DBConnectionPool(settings.DATABASE_URL),
        custom_field_api_mapping=client.fetch_deal_fields_mapping(),
        all_stages_details=client.fetch_all_stages_details()
    )
    
    # Testar diferentes tamanhos
    batch_sizes = [300, 500, 750, 1000, 1500]
    results = []
    
    for size in batch_sizes:
        etl = ETLService(client, repository, batch_size=size)
        result = etl.run_etl()
        
        results.append({
            'batch_size': size,
            'duration': result['duration_seconds'],
            'throughput': result['total_loaded'] / result['duration_seconds'],
            'memory_peak': result['peak_memory_mb']
        })

    # Analisar resultados
    df = pd.DataFrame(results)
    optimal = df[df['throughput'] == df['throughput'].max()]
    print(f"Optimal batch size: {optimal['batch_size'].values[0]}")
    df.to_csv('batch_size_analysis.csv', index=False)

if __name__ == "__main__":
    run_batch_size_experiment()