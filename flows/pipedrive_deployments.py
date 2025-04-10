import os
from datetime import timedelta
from prefect.deployments import Deployment
from prefect.server.schemas.schedules import IntervalSchedule, CronSchedule
from prefect_kubernetes.jobs import KubernetesJob

# Importar TODOS os fluxos
from flows.pipedrive_metabase_etl import (
    main_etl_flow,
    backfill_stage_history_flow,
    batch_size_experiment_flow
)
# Importar novos fluxos de sync
from flows.pipedrive_sync_aux import (
    sync_pipedrive_users_flow,
    sync_pipedrive_persons_orgs_flow,
    sync_pipedrive_stages_pipelines_flow
)

# --- Configuração Comum da Infraestrutura K8s Job ---
K8S_IMAGE_NAME = "pipedrive_metabase_integration-etl:latest"
K8S_NAMESPACE = "default" 
WORK_QUEUE_NAME = "kubernetes" 

# Definir recursos padrão para os jobs de fluxo
DEFAULT_JOB_RESOURCES = {
    "requests": {"memory": "1Gi", "cpu": "500m"},
    "limits": {"memory": "4Gi", "cpu": "2"}
}

# Definir Init Containers padrão para aguardar dependências
DEFAULT_INIT_CONTAINERS = [
    {
        "name": "wait-for-db",
        "image": "busybox:1.36",
        "command": ['sh', '-c', 'echo Waiting for db service...; while ! nc -z -w 1 db 5432; do sleep 2; done; echo DB ready.'],
    },
    {
        "name": "wait-for-redis",
        "image": "busybox:1.36",
        "command": ['sh', '-c', 'echo Waiting for redis service...; while ! nc -z -w 1 redis 6379; do sleep 2; done; echo Redis ready.'],
    },
     {
        "name": "wait-for-orion",
        "image": "curlimages/curl:latest", 
        "command": ['sh', '-c', 'echo Waiting for orion service...; until curl -sf http://prefect-orion:4200/api/health > /dev/null; do echo -n "."; sleep 3; done; echo Orion ready.'],
    }
]

# Definir variáveis de ambiente e secrets comuns
DEFAULT_ENV_FROM = [
    {"secretRef": {"name": "app-secrets"}},
    {"secretRef": {"name": "db-secrets"}},
]
PUSHGATEWAY_SVC_ADDRESS = os.getenv("PUSHGATEWAY_ADDRESS", "pushgateway:9091")

DEFAULT_ENV = {
    "PUSHGATEWAY_ADDRESS": PUSHGATEWAY_SVC_ADDRESS,
    "PREFECT_API_URL": "http://prefect-orion:4200/api"
}

k8s_job_infra = KubernetesJob(
    image=K8S_IMAGE_NAME,
    namespace=K8S_NAMESPACE,
    job=KubernetesJob.job_template(
        metadata={
            "labels": {"app.kubernetes.io/created-by": "prefect"}
        },
        spec={
            "template": {
                "spec": {
                    "initContainers": DEFAULT_INIT_CONTAINERS,
                    "containers": [
                        {
                            "name": "prefect-job", 
                            "resources": DEFAULT_JOB_RESOURCES,
                            "envFrom": DEFAULT_ENV_FROM,       
                            "env": [ {"name": k, "value": v} for k, v in DEFAULT_ENV.items() ], 
                        }
                    ],
                }
            }
        }
    ),
    # Timeout para esperar o Job K8s INICIAR (não completar)
    job_watch_timeout_seconds=120,
)

# --- Deployment para Main Sync ---
main_sync_deployment = Deployment.build_from_flow(
    flow=main_etl_flow, 
    name="Pipedrive Sync",
    description="Sincroniza deals recentes do Pipedrive com o banco de dados, usando lookups no DB.",
    version="2.0", 
    tags=["pipedrive", "sync", "etl", "main"],
    schedule=IntervalSchedule(interval=timedelta(minutes=30)), 
    parameters={}, 
    infrastructure=k8s_job_infra,
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
    # SEM schedule inicial, será disparado por automação
    schedule=None,
    # Parâmetros padrão para este deployment
    parameters={"daily_deal_limit": 10000, "db_batch_size": 1000},
    infrastructure=k8s_job_infra,
    work_queue_name=WORK_QUEUE_NAME,
)

# Sync Users
users_sync_deployment = Deployment.build_from_flow(
    flow=sync_pipedrive_users_flow,
    name="Sync Pipedrive Users",
    description="Sincroniza a tabela pipedrive_users com a API.",
    version="1.0",
    tags=["pipedrive", "sync", "aux", "users"],
    schedule=CronSchedule(cron="0 3 * * *", timezone="America/Sao_Paulo"), 
    parameters={},
    infrastructure=k8s_job_infra.duplicate(update={ 
         "job": KubernetesJob.job_template(
             spec={ "template": { "spec": {
                 "initContainers": DEFAULT_INIT_CONTAINERS,
                 "containers": [{
                     "name": "prefect-job",
                     "resources": {
                         "requests": {"memory": "512Mi", "cpu": "250m"},
                         "limits": {"memory": "1Gi", "cpu": "500m"}
                     },
                     "envFrom": DEFAULT_ENV_FROM, "env": [ {"name": k, "value": v} for k, v in DEFAULT_ENV.items() ]
                 }]
             }}}
         )
    }),
    work_queue_name=WORK_QUEUE_NAME,
    concurrency_limit=1
)

# Sync Persons & Orgs
persons_orgs_sync_deployment = Deployment.build_from_flow(
    flow=sync_pipedrive_persons_orgs_flow,
    name="Sync Pipedrive Persons and Orgs",
    description="Sincroniza as tabelas pipedrive_persons e pipedrive_organizations.",
    version="1.0",
    tags=["pipedrive", "sync", "aux", "persons", "orgs"],
    schedule=IntervalSchedule(interval=timedelta(hours=4)),
    parameters={},
    infrastructure=k8s_job_infra, 
    work_queue_name=WORK_QUEUE_NAME,
    concurrency_limit=1
)

# Sync Stages & Pipelines
stages_pipelines_sync_deployment = Deployment.build_from_flow(
    flow=sync_pipedrive_stages_pipelines_flow,
    name="Sync Pipedrive Stages & Pipelines",
    description="Sincroniza as tabelas pipedrive_stages e pipedrive_pipelines.",
    version="1.0",
    tags=["pipedrive", "sync", "aux", "stages", "pipelines"],
     schedule=CronSchedule(cron="0 4 * * *", timezone="America/Sao_Paulo"),
    parameters={},
    infrastructure=users_sync_deployment.infrastructure,
    work_queue_name=WORK_QUEUE_NAME,
    concurrency_limit=1
)


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
    infrastructure=k8s_job_infra.duplicate(
        update={
            "job": KubernetesJob.job_template(
                metadata={
                    "labels": {"app.kubernetes.io/created-by": "prefect"}
                },
                spec={
                    "template": {
                        "spec": {
                            "initContainers": DEFAULT_INIT_CONTAINERS,
                            "containers": [
                                {
                                    "name": "prefect-job",
                                    "resources": {
                                        "requests": {"memory": "2Gi", "cpu": "1"},
                                        "limits": {"memory": "8Gi", "cpu": "2"}
                                    },
                                    "envFrom": DEFAULT_ENV_FROM,
                                    "env": [{"name": k, "value": v} for k, v in DEFAULT_ENV.items()],
                                }
                            ],
                        }
                    }
                }
            )
        }
    ),
    work_queue_name=WORK_QUEUE_NAME,
)

# Bloco para aplicar os deployments
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