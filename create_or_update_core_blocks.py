#!/usr/bin/env python3
import os
from dotenv import load_dotenv
import structlog

from prefect.blocks.system import Secret, JSON
from prefect_docker.host import DockerHost
from prefect_docker.credentials import DockerRegistryCredentials
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
    try:
        Secret(value=pat).save(name="github-access-token", overwrite=True)
        log.info("Saved Secret block 'github-access-token'")
    except Exception:
        log.exception("Failed to save Secret block 'github-access-token'")
else:
    log.warning("GITHUB_PAT not set; skipping Secret block")

# ─── 2. JSON: Postgres Pool ────────────────────────────────────────────────────
db_cfg = {
    "dsn": os.getenv("DATABASE_URL", ""),
    "minconn": int(os.getenv("DB_MIN_CONN", 1)),
    "maxconn": int(os.getenv("DB_MAX_CONN", 10)),
}
try:
    JSON(value=db_cfg).save(name="postgres-pool", overwrite=True)
    log.info("Saved JSON block 'postgres-pool'")
except Exception:
    log.exception("Failed to save JSON block 'postgres-pool'")

# ─── 3. JSON: Redis Cache ──────────────────────────────────────────────────────
redis_cfg = {"connection_string": os.getenv("REDIS_URL", "")}
try:
    JSON(value=redis_cfg).save(name="redis-cache", overwrite=True)
    log.info("Saved JSON block 'redis-cache'")
except Exception:
    log.exception("Failed to save JSON block 'redis-cache'")

# ─── 4. DockerRegistryCredentials Block ────────────────────────────────────────
docker_user = os.getenv("DOCKER_USER")
docker_pass = os.getenv("DOCKER_PASS")
docker_url  = os.getenv("DOCKER_REGISTRY_URL", "")

if docker_user and docker_pass:
    try:
        DockerRegistryCredentials(
            username=docker_user,
            password=docker_pass,
            registry_url=docker_url
        ).save(name="docker-registry", overwrite=True)
        log.info("Saved DockerRegistryCredentials block 'docker-registry'")
    except Exception:
        log.exception("Failed to save DockerRegistryCredentials block 'docker-registry'")
else:
    log.info("DOCKER_USER/PASS not set; skipping DockerRegistryCredentials block")

# ─── 5. DockerHost Block ───────────────────────────────────────────────────────
try:
    DockerHost().save(name="docker-host", overwrite=True)
    log.info("Saved DockerHost block 'docker-host'")
except Exception:
    log.exception("Failed to save DockerHost block 'docker-host'")

# ─── 6. DockerWorker Block ─────────────────────────────────────────────────────
# Only pass registry_credentials (host is implicit)
worker_kwargs = {}
if docker_user:
    worker_kwargs["registry_credentials"] = DockerRegistryCredentials.load("docker-registry")

try:
    DockerWorker(**worker_kwargs).save(name="docker-worker", overwrite=True)
    log.info("Saved DockerWorker block 'docker-worker'")
except Exception:
    log.exception("Failed to save DockerWorker block 'docker-worker'")

log.info("Prefect block setup complete")
