#!/usr/bin/env python3
import os
import sys
import json
import subprocess

from dotenv import load_dotenv
import structlog
from prefect.blocks.system import Secret, JSON

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
GITHUB_PAT = os.getenv("GITHUB_PAT")
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

# ─── 2. JSON: Postgres Pool ────────────────────────────────────────────────────
db_config = {
    "dsn": os.getenv("DATABASE_URL", ""),
    "minconn": int(os.getenv("DB_MIN_CONN", 1)),
    "maxconn": int(os.getenv("DB_MAX_CONN", 10)),
}
try:
    JSON(value=db_config).save(
        name="postgres-pool", overwrite=True
    )
    log.info("Saved JSON block 'postgres-pool'")
except Exception:
    log.exception("Failed to save JSON block 'postgres-pool'")

# ─── 3. JSON: Redis Cache ──────────────────────────────────────────────────────
redis_config = {
    "connection_string": os.getenv("REDIS_URL", "")
}
try:
    JSON(value=redis_config).save(
        name="redis-cache", overwrite=True
    )
    log.info("Saved JSON block 'redis-cache'")
except Exception:
    log.exception("Failed to save JSON block 'redis-cache'")

# ─── 4. BLOCK CREATE via CLI: Docker Registry ─────────────────────────────────
DOCKER_USER = os.getenv("DOCKER_USER")
DOCKER_PASS = os.getenv("DOCKER_PASS")
DOCKER_REGISTRY_URL = os.getenv("DOCKER_REGISTRY_URL", "")

if DOCKER_USER and DOCKER_PASS:
    try:
        subprocess.run(
            [
                "prefect", "block", "create", "docker-registry", "docker-registry",
                "--param", f"username={DOCKER_USER}",
                "--param", f"password={DOCKER_PASS}",
                "--param", f"registry_url={DOCKER_REGISTRY_URL}",
                "--overwrite"
            ],
            check=True
        )
        log.info("Created/Updated block 'docker-registry'")
    except subprocess.CalledProcessError:
        log.exception("Failed to create 'docker-registry' block")
else:
    log.warn("DOCKER_USER/PASS not set; skipping 'docker-registry' block")

# ─── 5. BLOCK CREATE via CLI: Docker Container Variants ───────────────────────
ETL_IMAGE = os.getenv("ETL_IMAGE", "pipedrive_metabase_integration-etl:latest")
COMMON_ENV = json.dumps({
    "PREFECT_API_URL": os.getenv("PREFECT_API_URL"),
    "PUSHGATEWAY_ADDRESS": os.getenv("PUSHGATEWAY_ADDRESS"),
})
COMMON_VOLUMES = json.dumps(["/var/run/docker.sock:/var/run/docker.sock"])

containers = [
    ("default-docker-container", 0.5, "1Gi"),
    ("experiment-docker-container", 1, "2Gi"),
    ("light-sync-docker-container", 0.25, "512Mi"),
]

for name, cpu, mem in containers:
    cmd = [
        "prefect", "block", "create", "docker-container", name,
        "--param", f"image={ETL_IMAGE}",
        "--param", f"env={COMMON_ENV}",
        "--param", f"volumes={COMMON_VOLUMES}",
        "--param", f"cpu_limit={cpu}",
        "--param", f"memory_limit={mem}",
        "--param", "auto_remove=True",
        "--param", "stream_output=True",
        "--param", "image_pull_policy=if-not-present",
        "--overwrite"
    ]
    # If we created a registry block, point at it
    if DOCKER_USER:
        cmd += ["--param", "image_registry=docker-registry"]
    try:
        subprocess.run(cmd, check=True)
        log.info(f"Created/Updated DockerContainer block '{name}'")
    except subprocess.CalledProcessError:
        log.exception(f"Failed to create block 'docker-container/{name}'")

log.info("Prefect block setup complete")
