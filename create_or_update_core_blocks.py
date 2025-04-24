#!/usr/bin/env python3
import asyncio
import os
from typing import Any, Dict, List, Optional, Tuple

from dotenv import load_dotenv
import structlog

from prefect.blocks.core import Block 
from prefect.blocks.system import JSON, Secret
from prefect_docker.containers import create_docker_container
from prefect_docker.credentials import DockerRegistryCredentials
from prefect_docker.host import DockerHost

# --- Constantes ---
GITHUB_SECRET_BLOCK_NAME = "github-access-token"
POSTGRES_JSON_BLOCK_NAME = "postgres-pool"
REDIS_JSON_BLOCK_NAME = "redis-cache"
DOCKER_REGISTRY_BLOCK_NAME = "docker-registry"
DOCKER_HOST_BLOCK_NAME = "docker-host"
DEFAULT_NETWORK_NAME = "prefect_internal_network"

# --- Configuração do Logging ---
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
log = structlog.get_logger(__name__)


async def save_block_safe(block_instance: Block, name: str):
    """Tenta salvar um bloco e loga erros."""
    try:
        await block_instance.save(name=name, overwrite=True)
        log.info(f"Block '{name}' saved successfully.")
        return block_instance
    except Exception as e:
        log.error(f"Failed to save block '{name}'", error=str(e), exc_info=True)
        return None


async def load_block_safe(block_class: type[Block], name: str) -> Optional[Block]:
    """Tenta carregar um bloco e loga erros."""
    try:
        loaded_block = await block_class.load(name=name)
        log.info(f"Block '{name}' loaded successfully.")
        return loaded_block
    except ValueError:
        log.warn(f"Block '{name}' not found or failed to load.")
    except Exception as e:
        log.error(f"Error loading block '{name}'", error=str(e), exc_info=True)
    return None


async def setup_all_blocks():
    """Função principal assíncrona para criar/atualizar todos os blocos."""
    log.info("--- Starting Prefect Block Setup ---")
    load_dotenv()

    # --- Recupera Variáveis de Ambiente ---
    github_pat = os.getenv("GITHUB_PAT")
    database_url = os.getenv("DATABASE_URL")
    db_min_conn = int(os.getenv("DB_MIN_CONN", 1))
    db_max_conn = int(os.getenv("DB_MAX_CONN", 10))
    redis_url = os.getenv("REDIS_URL")
    docker_user = os.getenv("DOCKER_USER")
    docker_pass = os.getenv("DOCKER_PASS")
    docker_registry_url = os.getenv("DOCKER_REGISTRY_URL")
    etl_image = os.getenv("ETL_IMAGE")
    pushgateway_address = os.getenv("PUSHGATEWAY_ADDRESS", "pushgateway:9091")
    docker_network_name = os.getenv("COMPOSE_NETWORK_NAME", DEFAULT_NETWORK_NAME)

    # --- 1. Criação/Atualização de Blocos Síncronos ---
    log.info("--- Setting up standard blocks ---")

    # GitHub PAT Secret
    if github_pat:
        await save_block_safe(Secret(value=github_pat), GITHUB_SECRET_BLOCK_NAME)
    else:
        log.warn(f"{GITHUB_SECRET_BLOCK_NAME}: GITHUB_PAT not set, skipping.")

    # Postgres Pool JSON
    if database_url:
        db_cfg = {"dsn": database_url, "minconn": db_min_conn, "maxconn": db_max_conn}
        await save_block_safe(JSON(value=db_cfg), POSTGRES_JSON_BLOCK_NAME)
    else:
        log.warn(f"{POSTGRES_JSON_BLOCK_NAME}: DATABASE_URL not set, skipping.")

    # Redis Cache JSON
    if redis_url:
        redis_cfg = {"connection_string": redis_url}
        await save_block_safe(JSON(value=redis_cfg), REDIS_JSON_BLOCK_NAME)
    else:
        log.warn(f"{REDIS_JSON_BLOCK_NAME}: REDIS_URL not set, skipping.")

    creds_block_saved: Optional[DockerRegistryCredentials] = None
    if docker_user and docker_pass:
        creds_block_instance = DockerRegistryCredentials(
            username=docker_user,
            password=docker_pass,
            registry_url=docker_registry_url,
            reauth=True,
        )
        creds_block_saved = await save_block_safe(creds_block_instance, DOCKER_REGISTRY_BLOCK_NAME)
    else:
        log.info(f"{DOCKER_REGISTRY_BLOCK_NAME}: DOCKER_USER/PASS not set, skipping.")

    host_block_saved: Optional[DockerHost] = await save_block_safe(DockerHost(), DOCKER_HOST_BLOCK_NAME)
    if not host_block_saved:
        log.error(f"Failed to save {DOCKER_HOST_BLOCK_NAME}. Cannot continue with DockerContainer blocks.")
        return 

    # --- 2. Criação/Atualização de Blocos DockerContainer (Async) ---
    log.info("--- Setting up DockerContainer blocks ---")

    if not etl_image:
        log.error("ETL_IMAGE environment variable not set. Cannot create DockerContainer blocks.")
        return

    host_block = await load_block_safe(DockerHost, DOCKER_HOST_BLOCK_NAME)
    if not host_block:
        log.error(f"Failed to load required block {DOCKER_HOST_BLOCK_NAME}. Aborting DockerContainer setup.")
        return

    creds_block_loaded: Optional[DockerRegistryCredentials] = None
    if creds_block_saved:
        creds_block_loaded = await load_block_safe(DockerRegistryCredentials, DOCKER_REGISTRY_BLOCK_NAME)

    flow_run_api_url = "http://prefect-orion:4200/api"
    common_env: Dict[str, Optional[str]] = {
        "PREFECT_API_URL": flow_run_api_url,
        "PUSHGATEWAY_ADDRESS": pushgateway_address,
    }
    common_volumes: List[str] = ["/var/run/docker.sock:/var/run/docker.sock"]
    log.info(f"Flow run containers will use API URL: {flow_run_api_url}")
    log.info(f"Flow run containers will use Docker Network: {docker_network_name}")

    variants: List[Tuple[str, float, str]] = [
        ("default-docker-container", 0.5, "1Gi"),
        ("experiment-docker-container", 1.0, "2Gi"),
        ("light-sync-docker-container", 0.25, "512Mi"),
    ]

    for slug, cpu, mem in variants:
        log.info(f"Processing DockerContainer block: {slug}")
        try:
            await create_docker_container(
                name=slug,
                image=etl_image,
                image_pull_policy="if-not-present", 
                host=host_block,
                registry_credentials=creds_block_loaded, 
                env=common_env,
                volumes=common_volumes,
                network_mode=docker_network_name,
                cpu_limit=cpu,
                memory_limit=mem,
                auto_remove=True, 
                stream_output=True, 
                overwrite=True, 
            )
            log.info(f"Successfully saved DockerContainer block '{slug}'")
        except Exception as e:
            log.error(f"Failed to save DockerContainer block '{slug}'", error=str(e), exc_info=True)

    log.info("--- Prefect Block Setup Finished ---")


if __name__ == "__main__":
    asyncio.run(setup_all_blocks())