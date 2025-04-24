#!/usr/bin/env python3
import os
from dotenv import load_dotenv
import structlog
from prefect.blocks.system import Secret, JSON
from prefect_docker.credentials import DockerRegistryCredentials
from prefect_docker.host import DockerHost
from prefect_docker.worker import DockerWorker
from prefect_docker.containers import create_docker_container

# ─── Configure Logging ────────────────────────────────────────────────────────
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

# ─── Load .env ────────────────────────────────────────────────────────────────
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

# ─── 4. DockerRegistryCredentials (private registry) ──────────────────────────
docker_user = os.getenv("DOCKER_USER")
docker_pass = os.getenv("DOCKER_PASS")
docker_url  = os.getenv("DOCKER_REGISTRY_URL", "")
if docker_user and docker_pass:
    DockerRegistryCredentials(
        username=docker_user,
        password=docker_pass,
        registry_url=docker_url,
        reauth=True
    ).save(name="docker-registry", overwrite=True)
    log.info("Saved DockerRegistryCredentials block 'docker-registry'")
else:
    log.info("No DOCKER_USER/PASS; skipping DockerRegistryCredentials block")

# ─── 5. DockerHost ─────────────────────────────────────────────────────────────
DockerHost().save(name="docker-host", overwrite=True)
log.info("Saved DockerHost block 'docker-host'")

# ─── 6. DockerWorker ───────────────────────────────────────────────────────────
DockerWorker(work_pool_name="docker-pool").save(name="docker-pool", overwrite=True)
log.info("Saved DockerWorker block 'docker-pool'")

# ─── 7. DockerContainer blocks para ETL, experiment e light-sync ──────────────
etl_image = os.getenv("ETL_IMAGE", "pipedrive_metabase_integration-etl:latest")
common_env = {
    "PREFECT_API_URL": os.getenv("PREFECT_API_URL", ""),
    "PUSHGATEWAY_ADDRESS": os.getenv("PUSHGATEWAY_ADDRESS", "")
}
common_volumes = ["/var/run/docker.sock:/var/run/docker.sock"]

variants = [
    ("default-docker-container",    0.5,  "1Gi"),
    ("experiment-docker-container", 1.0,  "2Gi"),
    ("light-sync-docker-container", 0.25, "512Mi"),
]

for name, cpu, mem in variants:
    create_docker_container(
        name=name,
        image=etl_image,
        env=common_env,
        volumes=common_volumes,
        cpu_limit=cpu,
        memory_limit=mem,
        auto_remove=True,
        stream_output=True,
        image_pull_policy="if-not-present",
        image_registry="docker-registry" if docker_user else None
    )
    log.info(f"Saved DockerContainer block '{name}'")

log.info("Prefect block setup complete")
