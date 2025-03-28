import psycopg2.pool
from infrastructure.config.settings import settings

class DBConnectionPool:
    def __init__(self, minconn=1, maxconn=10):
        self.pool = psycopg2.pool.SimpleConnectionPool(
            minconn, maxconn, dsn=settings.DATABASE_URL
        )

    def get_connection(self):
        return self.pool.getconn()

    def release_connection(self, conn):
        self.pool.putconn(conn)

    def closeall(self):
        self.pool.closeall()
