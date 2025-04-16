import psycopg2.pool

class DBConnectionPool:
    def __init__(self, dsn: str, minconn: int = 1, maxconn: int = 10):
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

    def num_active(self) -> int:
        """
        Retorna o número de conexões atualmente emprestadas (ativas).
        """
        try:
            return len(self.pool._used)
        except Exception:
            return 0

    def num_idle(self) -> int:
        """
        Retorna o número de conexões disponíveis (ociosas) no pool.
        """
        try:
            return len(self.pool._pool)
        except Exception:
            return 0
