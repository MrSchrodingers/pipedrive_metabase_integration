from typing import List, Dict, Optional, Set, Any
from psycopg2 import sql, pool, extras
import structlog
import time

from infrastructure.db.schema_manager import SchemaManager

UNKNOWN_NAME = "Desconhecido"

class BaseLookupRepository:
    """
    Base class for lookup table repositories providing common upsert and query logic.
    Subclasses must define TABLE_NAME, ID_COLUMN, COLUMNS, and optionally NAME_COLUMN, INDEXES.
    """
    TABLE_NAME: str = ""
    ID_COLUMN: str = ""
    NAME_COLUMN: Optional[str] = None 
    COLUMNS: Dict[str, str] = {} # { column_name: type_definition_str }
    INDEXES: Dict[str, str] = {} # { index_name: column_definition_str }

    def __init__(self, db_pool: pool.SimpleConnectionPool, schema_manager: SchemaManager):
        if not self.TABLE_NAME or not self.ID_COLUMN or not self.COLUMNS:
            raise NotImplementedError(
                f"{self.__class__.__name__} must define TABLE_NAME, ID_COLUMN, and COLUMNS"
            )
        self.db_pool = db_pool
        self.schema_manager = schema_manager
        self.log = structlog.get_logger(__name__).bind(repository=self.__class__.__name__)

    def initialize_schema(self):
        """Initializes the schema for this lookup table using SchemaManager."""
        self.schema_manager.ensure_table_exists(
            table_name=self.TABLE_NAME,
            column_definitions=list(self.COLUMNS.items()),
            primary_key=self.ID_COLUMN,
            indexes=self.INDEXES
        )
        self.log.info("Schema initialized", table_name=self.TABLE_NAME)

    def _upsert_lookup_data(self, data: List[Dict[str, Any]], data_columns: List[str]) -> int:
        """
        Generic upsert logic for lookup tables using ON CONFLICT DO UPDATE.
        Handles updating 'last_synced_at' if present.
        Returns the number of rows affected by the upsert operation.
        """
        if not data:
            self.log.debug("No data provided to upsert", table_name=self.TABLE_NAME)
            return 0

        conn = None
        rows_affected = 0
        start_time = time.monotonic()

        valid_data_columns = [col for col in data_columns if col in self.COLUMNS]
        if len(valid_data_columns) != len(data_columns):
             self.log.warning("Some provided data columns are not in the table schema definition",
                              provided=data_columns, defined=list(self.COLUMNS.keys()))

        if not valid_data_columns:
             self.log.error("No valid columns found for upsert after schema check.")
             return 0

        try:
            conn = self.db_pool.get_connection()
            with conn.cursor() as cur:
                table_id = sql.Identifier(self.TABLE_NAME)
                col_ids = sql.SQL(', ').join(map(sql.Identifier, valid_data_columns))

                update_cols_list = [
                    sql.SQL("{col} = EXCLUDED.{col}").format(col=sql.Identifier(col))
                    for col in valid_data_columns if col != self.ID_COLUMN
                ]

                if "last_synced_at" in self.COLUMNS:
                    update_cols_list.append(sql.SQL("last_synced_at = NOW()"))

                if not update_cols_list:
                     self.log.warning("No columns specified for update in ON CONFLICT clause.", table_name=self.TABLE_NAME)
                     pass

                update_assignments = sql.SQL(', ').join(update_cols_list)

                upsert_sql_template = sql.SQL("""
                    INSERT INTO {table} ({insert_cols}) VALUES %s
                    ON CONFLICT ({pk_col}) DO UPDATE SET {update_assignments}
                """).format(
                    table=table_id,
                    insert_cols=col_ids,
                    pk_col=sql.Identifier(self.ID_COLUMN),
                    update_assignments=update_assignments if update_cols_list else sql.SQL("NOTHING") 
                )

                values_tuples = []
                for d in data:
                     row = tuple(d.get(col) for col in valid_data_columns)
                     values_tuples.append(row)

                extras.execute_values(cur, upsert_sql_template.as_string(cur), values_tuples, page_size=500)
                rows_affected = cur.rowcount 
                conn.commit()
                duration = time.monotonic() - start_time
                self.log.info(
                    "Upsert successful", table_name=self.TABLE_NAME,
                    records_in=len(data), rows_affected=rows_affected, duration_sec=f"{duration:.3f}s"
                )
                return rows_affected
        except Exception as e:
            if conn: conn.rollback()
            self.log.error("Upsert failed", table_name=self.TABLE_NAME, error=str(e), exc_info=True)
            raise 
        finally:
            if conn:
                self.db_pool.release_connection(conn)

    def get_name_map_for_ids(self, ids: Set[int]) -> Dict[int, str]:
        """
        Fetches a mapping of ID to Name for the given set of IDs.
        Requires NAME_COLUMN to be defined in the subclass.
        """
        if not self.NAME_COLUMN:
            self.log.warning("NAME_COLUMN not defined, cannot fetch name map.", table_name=self.TABLE_NAME)
            return {}
        if not ids:
            return {}

        conn = None
        start_time = time.monotonic()
        try:
            conn = self.db_pool.get_connection()
            with conn.cursor() as cur:
                query = sql.SQL("SELECT {id_col}, {name_col} FROM {table} WHERE {id_col} = ANY(%s)").format(
                    id_col=sql.Identifier(self.ID_COLUMN),
                    name_col=sql.Identifier(self.NAME_COLUMN),
                    table=sql.Identifier(self.TABLE_NAME)
                )
                cur.execute(query, (list(ids),))
                results = {row[0]: str(row[1] or UNKNOWN_NAME) for row in cur.fetchall()} # Ensure name is string, handle None
                duration = time.monotonic() - start_time
                self.log.debug("Fetched name map", table_name=self.TABLE_NAME, ids_count=len(ids), found_count=len(results), duration_sec=f"{duration:.3f}s")
                return results
        except Exception as e:
            self.log.error("Failed to get name map", table_name=self.TABLE_NAME, error=str(e), exc_info=True)
            return {}
        finally:
            if conn:
                self.db_pool.release_connection(conn)