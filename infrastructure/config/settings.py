# settings.py
import os
from dotenv import load_dotenv

load_dotenv()

class Settings:
    """Configurações da aplicação (env vars)."""
    # — API Keys
    PIPEDRIVE_API_KEY     = os.getenv("PIPEDRIVE_API_KEY")
    GITHUB_PAT            = os.getenv("GITHUB_PAT")

    # — Postgres
    POSTGRES_DB           = os.getenv("POSTGRES_DB")
    POSTGRES_USER         = os.getenv("POSTGRES_USER")
    POSTGRES_PASSWORD     = os.getenv("POSTGRES_PASSWORD")
    POSTGRES_HOST         = os.getenv("POSTGRES_HOST", "db")
    POSTGRES_PORT         = os.getenv("POSTGRES_PORT", "5432")
    DATABASE_URL          = os.getenv("DATABASE_URL")

    # — Redis
    REDIS_URL             = os.getenv("REDIS_URL", "redis://redis:6379/0")

    # — Docker / ETL
    ETL_IMAGE             = os.getenv("ETL_IMAGE")

    # — Docker registry (blocks)
    DOCKER_USER           = os.getenv("DOCKER_USER")
    DOCKER_PASS           = os.getenv("DOCKER_PASS")
    DOCKER_REGISTRY_URL   = os.getenv("DOCKER_REGISTRY_URL")

    # — Host ports (mapeados no docker-compose)
    HOST_PREFECT_PORT     = int(os.getenv("HOST_PREFECT_PORT"))
    HOST_METRICS_PORT     = int(os.getenv("HOST_METRICS_PORT"))
    HOST_GRAFANA_PORT     = int(os.getenv("HOST_GRAFANA_PORT"))
    HOST_POSTGRES_PORT    = int(os.getenv("HOST_POSTGRES_PORT"))

    # — Outros
    PUSHGATEWAY_ADDRESS   = os.getenv("PUSHGATEWAY_ADDRESS")

    BATCH_OPTIMIZER_CONFIG = {
        "memory_threshold": 0.8,
        "reduce_factor":    0.7,
        "duration_threshold": 30,
        "increase_factor":  1.2,
        "history_window":   5,
    }

settings = Settings()
