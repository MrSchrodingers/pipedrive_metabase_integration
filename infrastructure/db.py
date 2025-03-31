import psycopg2
from infrastructure.config.settings import settings

def get_db_connection():
    return psycopg2.connect(f"postgresql://{settings.POSTGRES_USER}:{settings.POSTGRES_PASSWORD}@{settings.POSTGRES_HOST}:{settings.POSTGRES_PORT}/{settings.POSTGRES_DB}")
