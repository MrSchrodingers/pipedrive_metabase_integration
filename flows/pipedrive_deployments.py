import os
from datetime import timedelta
from prefect.deployments import Deployment
from prefect.server.schemas.schedules import IntervalSchedule, CronSchedule
from prefect.infrastructure import DockerContainer

from flows.pipedrive_metabase_etl import (
    main_etl_flow,
    backfill_stage_history_flow,
    batch_size_experiment_flow
)
from flows.pipedrive_sync_aux import (
    sync_pipedrive_users_flow,
    sync_pipedrive_persons_orgs_flow,
    sync_pipedrive_stages_pipelines_flow
)

# --- Configurações Comuns ---
IMAGE_NAME = "pipedrive_metabase_integration-etl:latest"
WORK_QUEUE_NAME = "docker"

# Variáveis de ambiente comuns para dentro dos containers
COMMON_ENV = {
    "PUSHGATEWAY_ADDRESS": os.getenv("PUSHGATEWAY_ADDRESS", "pushgateway:9091"),
    "PREFECT_API_URL": os.getenv("PREFECT_API_URL", "http://prefect-orion:4200/api"),
}

# Infraestrutura DockerContainer
docker_infra = DockerContainer(
    image=IMAGE_NAME,
    env=COMMON_ENV,
    auto_remove=True
)

# --- Deployment para Main Sync ---
main_sync_deployment = Deployment.build_from_flow(
    flow=main_etl_flow,
    name="Pipedrive Sync",
    description="Sincroniza deals recentes do Pipedrive com o banco de dados, usando lookups no DB.",
    version="2.0",
    tags=["pipedrive", "sync", "etl", "main"],
    parameters={},
    infrastructure=docker_infra,
    work_queue_name=WORK_QUEUE_NAME,
    concurrency_limit=1
)

# --- Deployment para Backfill ---
backfill_deployment = Deployment.build_from_flow(
    flow=backfill_stage_history_flow,
    name="Pipedrive Backfill Stage History",
    description="Preenche o histórico de stages para deals antigos.",
    version="1.0",
    tags=["pipedrive", "backfill", "history"],
    schedule=None, 
    parameters={"daily_deal_limit": 10000, "db_batch_size": 1000},
    infrastructure=docker_infra,
    work_queue_name=WORK_QUEUE_NAME,
    concurrency_limit=1
)

# --- Sync Users ---
users_sync_deployment = Deployment.build_from_flow(
    flow=sync_pipedrive_users_flow,
    name="Sync Pipedrive Users",
    description="Sincroniza a tabela pipedrive_users com a API.",
    version="1.0",
    tags=["pipedrive", "sync", "aux", "users"],
    schedule=CronSchedule(cron="0 3 * * *", timezone="America/Sao_Paulo"),
    parameters={},
    infrastructure=docker_infra,
    work_queue_name=WORK_QUEUE_NAME,
    concurrency_limit=1
)

# --- Sync Persons & Orgs ---
persons_orgs_sync_deployment = Deployment.build_from_flow(
    flow=sync_pipedrive_persons_orgs_flow,
    name="Sync Pipedrive Persons and Orgs",
    description="Sincroniza as tabelas pipedrive_persons e pipedrive_organizations.",
    version="1.0",
    tags=["pipedrive", "sync", "aux", "persons", "orgs"],
    schedule=IntervalSchedule(interval=timedelta(hours=4)),
    parameters={},
    infrastructure=docker_infra,
    work_queue_name=WORK_QUEUE_NAME,
    concurrency_limit=1
)

# --- Sync Stages & Pipelines ---
stages_pipelines_sync_deployment = Deployment.build_from_flow(
    flow=sync_pipedrive_stages_pipelines_flow,
    name="Sync Pipedrive Stages and Pipelines",
    description="Sincroniza as tabelas pipedrive_stages e pipedrive_pipelines.",
    version="1.0",
    tags=["pipedrive", "sync", "aux", "stages", "pipelines"],
    schedule=CronSchedule(cron="0 4 * * *", timezone="America/Sao_Paulo"),
    parameters={},
    infrastructure=docker_infra,
    work_queue_name=WORK_QUEUE_NAME,
    concurrency_limit=1
)

# --- Batch Size Experiment ---
batch_experiment_deployment = Deployment.build_from_flow(
    flow=batch_size_experiment_flow,
    name="Batch Size Experiment",
    description="Testa diferentes tamanhos de batch para otimização de performance.",
    version="1.0",
    tags=["experiment", "batch-size", "optimization"],
    schedule=None,
    parameters={
        "batch_sizes": [300, 500, 750, 1000, 1500],
        "test_data_size": 10000
    },
    infrastructure=docker_infra,
    work_queue_name=WORK_QUEUE_NAME,
    concurrency_limit=1
)

# --- Aplicar todos os deployments ---
if __name__ == "__main__":
    print("Applying Pipedrive Sync deployment...")
    main_sync_deployment.apply()

    print("Applying Backfill deployment...")
    backfill_deployment.apply()

    print("Applying Batch Experiment deployment...")
    batch_experiment_deployment.apply()

    print("Applying Users Sync deployment...")
    users_sync_deployment.apply()

    print("Applying Persons & Orgs Sync deployment...")
    persons_orgs_sync_deployment.apply()

    print("Applying Stages & Pipelines Sync deployment...")
    stages_pipelines_sync_deployment.apply()

    print("\nDeployments aplicados com sucesso!")
    print(f"Garanta que um agente Prefect esteja rodando e escutando a fila '{WORK_QUEUE_NAME}'.")
    print(f"Exemplo: prefect agent start -q {WORK_QUEUE_NAME}")
    print("Configure as Automações na UI do Prefect para orquestrar os fluxos.")
