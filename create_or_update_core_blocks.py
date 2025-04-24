#!/usr/bin/env python3
import os
from dotenv import load_dotenv
import structlog

from prefect.blocks.system import Secret, JSON
from prefect_docker.credentials import DockerRegistryCredentials
from prefect_docker.host import DockerHost
from prefect_docker.worker import DockerWorker

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

# ─── 4. DockerRegistryCredentials Block ────────────────────────────────────────
docker_user = os.getenv("DOCKER_USER")
docker_pass = os.getenv("DOCKER_PASS")
docker_url  = os.getenv("DOCKER_REGISTRY_URL", "")

if docker_user and docker_pass:
    DockerRegistryCredentials(
        username=docker_user,
        password=docker_pass,
        registry_url=docker_url
    ).save(name="docker-registry", overwrite=True)
    log.info("Saved DockerRegistryCredentials block 'docker-registry'")
else:
    log.info("DOCKER_USER/PASS not set; skipping DockerRegistry block")

# ─── 5. DockerHost Block ───────────────────────────────────────────────────────
# Configure a local Docker host for container operations
DockerHost().save(name="docker-host", overwrite=True)
log.info("Saved DockerHost block 'docker-host'")

# ─── 6. DockerWorker Block ─────────────────────────────────────────────────────
# Use the DockerHost (and registry credentials if available) for flow runs
worker_kwargs = {"host": DockerHost.load("docker-host")}
if docker_user:
    worker_kwargs["registry_credentials"] = DockerRegistryCredentials.load("docker-registry")

DockerWorker(**worker_kwargs).save(name="docker-worker", overwrite=True)
log.info("Saved DockerWorker block 'docker-worker'")

log.info("Prefect block setup complete")
