import time
from typing import List, Dict, Optional, Tuple, Set
from psycopg2 import sql, pool
import structlog

class SchemaManager:
    SCHEMA_LOCK_ID = 47835 

    def __init__(self, db_pool: pool.SimpleConnectionPool):
        self.db_pool = db_pool
        self.log = structlog.get_logger(__name__).bind(service="SchemaManager")

    def _execute_with_lock(self, func, *args, **kwargs):
        """Executes a function within an advisory lock."""
        conn = None
        locked = False
        try:
            conn = self.db_pool.get_connection()
            with conn.cursor() as cur:
                cur.execute("SELECT pg_advisory_lock(%s)", (self.SCHEMA_LOCK_ID,))
                locked = True
                self.log.debug("Acquired schema lock", lock_id=self.SCHEMA_LOCK_ID)
                result = func(cur, *args, **kwargs)
                conn.commit()
                return result
        except Exception as e:
            if conn: conn.rollback()
            self.log.error("Schema operation failed", error=str(e), exc_info=True)
            raise
        finally:
            if conn:
                if locked:
                    try:
                        with conn.cursor() as unlock_cur:
                           unlock_cur.execute("SELECT pg_advisory_unlock(%s)", (self.SCHEMA_LOCK_ID,))
                        conn.commit() 
                        self.log.debug("Released schema lock", lock_id=self.SCHEMA_LOCK_ID)
                    except Exception as unlock_err:
                         self.log.error("Failed to release schema lock", error=str(unlock_err))
                self.db_pool.release_connection(conn)

    def ensure_table_exists(
        self,
        table_name: str,
        column_definitions: List[Tuple[str, str]], 
        primary_key: Optional[str] = None,
        indexes: Optional[Dict[str, str]] = None 
    ):
        """Ensures a table exists with the specified columns, PK, and indexes."""
        return self._execute_with_lock(
            self._ensure_table_exists_internal,
            table_name, column_definitions, primary_key, indexes
        )

    def _ensure_table_exists_internal(
        self, cur, table_name: str, column_definitions: List[Tuple[str, str]],
        primary_key: Optional[str], indexes: Optional[Dict[str, str]]
    ):
        table_id = sql.Identifier(table_name)
        exists = self._check_table_exists(cur, table_name)

        if not exists:
            self._create_table(cur, table_id, column_definitions, primary_key)
        else:
            self._add_missing_columns(cur, table_id, column_definitions)

        if indexes:
            self._create_indexes(cur, table_id, indexes)

    def ensure_columns_exist(self, table_name: str, expected_cols: Set[str]):
         """Adds missing columns to an existing table, defaulting to TEXT type."""
         # This simplified version just ensures existence, defaulting type
         # A more robust version might take Dict[str, str] with types
         return self._execute_with_lock(self._ensure_columns_exist_internal, table_name, expected_cols)

    def _ensure_columns_exist_internal(self, cur, table_name: str, expected_cols: Set[str]) -> bool:
         table_id = sql.Identifier(table_name)
         existing_cols = self._get_existing_columns(cur, table_name)
         missing_cols = expected_cols - existing_cols
         schema_changed = False
         if missing_cols:
             schema_changed = self._add_missing_columns(cur, table_id, [(col, "TEXT") for col in missing_cols])
         return schema_changed 

    def _check_table_exists(self, cur, table_name: str) -> bool:
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_schema = 'public' AND table_name = %s
            );
        """, (table_name,))
        return cur.fetchone()[0]

    def _get_existing_columns(self, cur, table_name: str) -> Set[str]:
        cur.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = %s;
        """, (table_name,))
        return {row[0] for row in cur.fetchall()}

    def _get_table_column_types(self, table_name: str) -> Dict[str, str]:
        """Helper to get column types for casting during UPSERT."""
        conn = None
        try:
             conn = self.db_pool.get_connection()
             with conn.cursor() as cur:
                 cur.execute("""
                     SELECT column_name, data_type
                     FROM information_schema.columns
                     WHERE table_schema = 'public' AND table_name = %s;
                 """, (table_name,))
                 return {row[0]: row[1].upper() for row in cur.fetchall()}
        except Exception as e:
            self.log.error("Failed to get column types", table_name=table_name, error=str(e))
            return {}
        finally:
             if conn: self.db_pool.release_connection(conn)


    def _create_table(self, cur, table_id: sql.Identifier, column_definitions: List[Tuple[str, str]], primary_key: Optional[str]):
        """Creates the table."""
        col_defs_sql = [
            sql.SQL("{} {}").format(sql.Identifier(name), sql.SQL(ctype))
            for name, ctype in column_definitions
        ]
        if primary_key:
             found = False
             for i, (name, ctype) in enumerate(column_definitions):
                 if name == primary_key:
                     col_defs_sql[i] = sql.SQL("{} {} PRIMARY KEY").format(sql.Identifier(name), sql.SQL(ctype.replace(" PRIMARY KEY", "")))
                     found = True
                     break
             if not found: 
                col_defs_sql.append(sql.SQL("PRIMARY KEY ({})").format(sql.Identifier(primary_key)))

        query = sql.SQL("CREATE TABLE {table} ({columns})").format(
            table=table_id, columns=sql.SQL(', ').join(col_defs_sql)
        )
        self.log.info("Creating table", table_name=table_id.strings[0])
        cur.execute(query)

    def _add_missing_columns(self, cur, table_id: sql.Identifier, columns_to_add: List[Tuple[str, str]]) -> bool:
        """Adds columns if they don't exist. Returns True if any columns were added."""
        if not columns_to_add:
            return False

        existing_cols = self._get_existing_columns(cur, table_id.strings[0])
        missing_cols_final = [(name, ctype) for name, ctype in columns_to_add if name not in existing_cols]

        if not missing_cols_final:
             return False

        alter_statements = [
            sql.SQL("ADD COLUMN IF NOT EXISTS {} {}").format(sql.Identifier(name), sql.SQL(ctype))
            for name, ctype in missing_cols_final
        ]
        if alter_statements:
            query = sql.SQL("ALTER TABLE {table} ").format(table=table_id) + sql.SQL(', ').join(alter_statements)
            self.log.info("Adding missing columns", table_name=table_id.strings[0], columns=[name for name, _ in missing_cols_final])
            cur.execute(query)
            return True
        return False

    def _create_indexes(self, cur, table_id: sql.Identifier, indexes: Dict[str, str]):
        """Creates indexes if they don't exist."""
        for idx_name, idx_definition in indexes.items():
            cur.execute("SELECT EXISTS (SELECT FROM pg_class WHERE relname = %s);", (idx_name,))
            if not cur.fetchone()[0]:
                 query = sql.SQL("CREATE INDEX IF NOT EXISTS {} ON {} ({})").format(
                     sql.Identifier(idx_name), table_id, sql.SQL(idx_definition)
                 )
                 self.log.info("Creating index", index_name=idx_name, table_name=table_id.strings[0])
                 cur.execute(query)