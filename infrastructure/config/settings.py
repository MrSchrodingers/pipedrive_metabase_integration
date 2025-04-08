import os
from dotenv import load_dotenv

load_dotenv()

class Settings:
    """
    Configurações da aplicação, lidas de variáveis de ambiente.
    """
    PIPEDRIVE_API_KEY = os.getenv("PIPEDRIVE_API_KEY")

    POSTGRES_DB = os.getenv("POSTGRES_DB")
    POSTGRES_USER = os.getenv("POSTGRES_USER")
    POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
    POSTGRES_HOST = os.getenv("POSTGRES_HOST")
    POSTGRES_PORT = os.getenv("POSTGRES_PORT")
    DATABASE_URL = os.getenv("DATABASE_URL")

    REDIS_URL = os.getenv("REDIS_URL")
    
    BATCH_OPTIMIZER_CONFIG = {
        'memory_threshold': 0.8,
        'reduce_factor': 0.7,
        'duration_threshold': 30,
        'increase_factor': 1.2,
        'history_window': 5
    }

settings = Settings()
