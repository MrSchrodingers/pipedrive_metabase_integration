from prometheus_client import Counter, Histogram

# Métricas de execução do ETL
etl_counter = Counter("etl_total_runs", "Total de execuções do ETL")
etl_failure_counter = Counter("etl_failures", "Total de falhas no ETL")
etl_duration = Histogram("etl_duration_seconds", "Duração do processo ETL em segundos")
