#!/usr/bin/env python3
import os
import json
import asyncio
from dotenv import load_dotenv
import structlog

from prefect.blocks.system import Secret, JSON
from prefect import get_client

# ─── Configure Logging ─────────────────────────────────────────────────────────
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

# ─── Load Environment ──────────────────────────────────────────────────────────
load_dotenv()
log.info("Starting Prefect block setup")

# ─── Read common env vars ──────────────────────────────────────────────────────
GITHUB_PAT            = os.getenv("GITHUB_PAT")
DATABASE_URL          = os.getenv("DATABASE_URL", "")
DB_MIN_CONN           = int(os.getenv("DB_MIN_CONN", 1))
DB_MAX_CONN           = int(os.getenv("DB_MAX_CONN", 10))
REDIS_URL             = os.getenv("REDIS_URL", "")
DOCKER_USER           = os.getenv("DOCKER_USER")
DOCKER_PASS           = os.getenv("DOCKER_PASS")
DOCKER_REGISTRY_URL   = os.getenv("DOCKER_REGISTRY_URL", "")
ETL_IMAGE             = os.getenv("ETL_IMAGE", "pipedrive_metabase_integration-etl:latest")
COMMON_ENV            = {
    "PREFECT_API_URL": os.getenv("PREFECT_API_URL"),
    "PUSHGATEWAY_ADDRESS": os.getenv("PUSHGATEWAY_ADDRESS"),
}
COMMON_VOLUMES        = ["/var/run/docker.sock:/var/run/docker.sock"]
CONTAINERS = [
    ("default-docker-container", 0.5, "1Gi"),
    ("experiment-docker-container", 1, "2Gi"),
    ("light-sync-docker-container", 0.25, "512Mi"),
]

async def main():
    # ─── 1. SECRET: GitHub PAT ─────────────────────────────────────────────────
    if GITHUB_PAT:
        try:
            Secret(value=GITHUB_PAT).save(
                name="github-access-token", overwrite=True
            )
            log.info("Saved Secret block 'github-access-token'")
        except Exception:
            log.exception("Failed to save Secret block 'github-access-token'")
    else:
        log.warn("GITHUB_PAT not set; skipping Secret block")

    # ─── 2. JSON: Postgres Pool ─────────────────────────────────────────────────
    try:
        JSON(value={
            "dsn":      DATABASE_URL,
            "minconn":  DB_MIN_CONN,
            "maxconn":  DB_MAX_CONN,
        }).save(name="postgres-pool", overwrite=True)
        log.info("Saved JSON block 'postgres-pool'")
    except Exception:
        log.exception("Failed to save JSON block 'postgres-pool'")

    # ─── 3. JSON: Redis Cache ───────────────────────────────────────────────────
    try:
        JSON(value={"connection_string": REDIS_URL}) \
            .save(name="redis-cache", overwrite=True)
        log.info("Saved JSON block 'redis-cache'")
    except Exception:
        log.exception("Failed to save JSON block 'redis-cache'")

    # ─── 4 & 5. Criar/Atualizar Blocos via API ─────────────────────────────────
    async with get_client() as client:
        # 4. Docker Registry Credentials
        if DOCKER_USER and DOCKER_PASS:
            try:
                await client.create_block_document(
                    name="docker-registry",
                    block_type_slug="docker-registry-credentials",
                    data={
                        "username":     DOCKER_USER,
                        "password":     DOCKER_PASS,
                        "registry_url": DOCKER_REGISTRY_URL,
                    },
                    overwrite=True,
                )
                log.info("Created/Updated block 'docker-registry'")
            except Exception:
                log.exception("Failed to create 'docker-registry' block")
        else:
            log.warn("DOCKER_USER/PASS not set; skipping registry block")

        # 5. Docker Container Variants
        for name, cpu, mem in CONTAINERS:
            try:
                container_data = {
                    "image":               ETL_IMAGE,
                    "env":                 COMMON_ENV,
                    "volumes":             COMMON_VOLUMES,
                    "cpu_limit":           cpu,
                    "memory_limit":        mem,
                    "auto_remove":         True,
                    "stream_output":       True,
                    "image_pull_policy":   "if-not-present",
                    # se quiser referenciar o bloco de creds, use:
                    "docker_registry_credentials": "docker-registry",
                }
                await client.create_block_document(
                    name=name,
                    block_type_slug="docker-container",
                    data=container_data,
                    overwrite=True,
                )
                log.info(f"Created/Updated block 'docker-container/{name}'")
            except Exception:
                log.exception(f"Failed to create 'docker-container/{name}' block")

    log.info("Prefect block setup complete")

if __name__ == "__main__":
    asyncio.run(main())
