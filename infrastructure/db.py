import psycopg2
from infrastructure.config.settings import settings

def get_db_connection():
    return psycopg2.connect(settings.DATABASE_URL)
