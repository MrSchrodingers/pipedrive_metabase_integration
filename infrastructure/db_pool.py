import psycopg2.pool

class DBConnectionPool:
    def __init__(self, dsn: str, minconn=1, maxconn=10):
        self.pool = psycopg2.pool.SimpleConnectionPool(
            minconn=minconn,
            maxconn=maxconn,
            dsn=dsn
        )

    def get_connection(self):
        return self.pool.getconn()

    def release_connection(self, conn):
        self.pool.putconn(conn)

    def closeall(self):
        self.pool.closeall()