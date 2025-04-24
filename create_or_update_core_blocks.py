#!/usr/bin/env python3
import os
from dotenv import load_dotenv
import structlog

from prefect.blocks.system import Secret, JSON
from prefect.infrastructure import DockerRegistry, DockerContainer, ImagePullPolicy

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

# ─── 1. SECRET: GitHub PAT ─────────────────────────────────────────────────────
if (pat := os.getenv("GITHUB_PAT")):
    Secret(value=pat).save(name="github-access-token", overwrite=True)
    log.info("Saved Secret block 'github-access-token'")
else:
    log.warn("GITHUB_PAT not set; skipping Secret block")

# ─── 2. JSON: Postgres Pool ────────────────────────────────────────────────────
db_cfg = {
    "dsn": os.getenv("DATABASE_URL", ""),
    "minconn": int(os.getenv("DB_MIN_CONN", 1)),
    "maxconn": int(os.getenv("DB_MAX_CONN", 10)),
}
JSON(value=db_cfg).save(name="postgres-pool", overwrite=True)
log.info("Saved JSON block 'postgres-pool'")

# ─── 3. JSON: Redis Cache ──────────────────────────────────────────────────────
redis_cfg = {"connection_string": os.getenv("REDIS_URL", "")}
JSON(value=redis_cfg).save(name="redis-cache", overwrite=True)
log.info("Saved JSON block 'redis-cache'")

# ─── 4. DOCKER REGISTRY Block ──────────────────────────────────────────────────
docker_user = os.getenv("DOCKER_USER")
docker_pass = os.getenv("DOCKER_PASS")
docker_url  = os.getenv("DOCKER_REGISTRY_URL", "")

if docker_user and docker_pass:
    DockerRegistry(
        username=docker_user,
        password=docker_pass,
        registry_url=docker_url,
        reauth=True
    ).save(name="docker-registry", overwrite=True)
    log.info("Saved DockerRegistry block 'docker-registry'")
else:
    log.info("DOCKER_USER/PASS not set; skipping DockerRegistry block")

# ─── 5. DOCKER CONTAINER Blocks ────────────────────────────────────────────────
etl_image = os.getenv("ETL_IMAGE", "")
common_env = {
    "PREFECT_API_URL": os.getenv("PREFECT_API_URL", ""),
    "PUSHGATEWAY_ADDRESS": os.getenv("PUSHGATEWAY_ADDRESS", "")
}
common_volumes = ["/var/run/docker.sock:/var/run/docker.sock"]

for name, cpu, mem in [
    ("default-docker-container",    0.5,   "1Gi"),
    ("experiment-docker-container", 1.0,   "2Gi"),
    ("light-sync-docker-container", 0.25, "512Mi"),
]:
    DockerContainer(
        image=etl_image,
        env=common_env,
        volumes=common_volumes,
        cpu_limit=cpu,
        memory_limit=mem,
        auto_remove=True,
        stream_output=True,
        image_pull_policy=ImagePullPolicy.IF_NOT_PRESENT,
        image_registry="docker-registry" if docker_user else None
    ).save(name=name, overwrite=True)
    log.info(f"Saved DockerContainer block '{name}'")

log.info("Prefect block setup complete")
