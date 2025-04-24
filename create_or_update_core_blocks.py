#!/usr/bin/env python3
import os
from dotenv import load_dotenv
import structlog
from prefect import flow, get_run_logger
from prefect.blocks.system import Secret, JSON
from prefect_docker.containers import (
    create_docker_container,
    start_docker_container,
    get_docker_container_logs,
)

# ─── Logging Setup ────────────────────────────────────────
try:
    structlog.configure(
        processors=[
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.dev.ConsoleRenderer(),
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )
except structlog.exceptions.AlreadyConfiguredError:
    pass
log = get_run_logger()

load_dotenv()

# ─── Flow Definition ──────────────────────────────────────
@flow(name="block-creator")
def create_or_update_blocks_flow():
    log.info("Iniciando criação/atualização de blocks Prefect")

    # 1. Cria o contêiner Docker que executa este script internamente
    container = create_docker_container(
        image=os.getenv("ETL_IMAGE", "pipedrive_metabase_integration-etl:latest"),
        command=["python", "/app/create_or_update_core_blocks.py"],
        name="prefect-block-creator",
        detach=True,
        volumes=[
            f"{os.getcwd()}:/app",               
            "/var/run/docker.sock:/var/run/docker.sock"  
        ],
        environment={
            "GITHUB_PAT": os.getenv("GITHUB_PAT", ""),
            "DATABASE_URL": os.getenv("DATABASE_URL", ""),
            "REDIS_URL": os.getenv("REDIS_URL", ""),
            "PREFECT_API_URL": os.getenv("PREFECT_API_URL", ""),
        },
    )
    log.info(f"Container criado: {container.id}")

    # 2. Inicia o contêiner para rodar o script de blocks
    start_docker_container(container_id=container.id)
    log.info("Container iniciado, aguardando conclusão")

    # 3. Captura e exibe os logs do contêiner
    logs = get_docker_container_logs(container_id=container.id, stream=False)
    log.info(f"Logs do block-creator container:\n{logs}")

    # 4. Remoção automática já configurada pelo detach e pelo fluxo de limpeza do Docker
    log.info("Criação/atualização de blocks concluída")

if __name__ == "__main__":
    create_or_update_blocks_flow()
