import time
import random
import csv
import json
from io import StringIO
from typing import List, Dict, Any, Optional, Set, Tuple
from datetime import datetime, timezone, date, time as time_obj 
from decimal import Decimal

from psycopg2 import sql, pool, extras
import pandas as pd 
import numpy as np
import structlog

from application.ports.data_repository_port import DataRepositoryPort
from infrastructure.db.schema_manager import SchemaManager
from application.utils.column_utils import normalize_column_name 

log = structlog.get_logger(__name__)

BASE_COLUMNS_DEFS = {
    "id": "TEXT PRIMARY KEY", "titulo": "TEXT", "creator_user_id": "INTEGER",
    "creator_user_name": "TEXT", "person_id": "INTEGER", "person_name": "TEXT",
    "stage_id": "INTEGER", "stage_name": "TEXT", "pipeline_id": "INTEGER",
    "pipeline_name": "TEXT", "owner_id": "INTEGER", "owner_name": "TEXT",
    "status": "TEXT", "value": "NUMERIC(18, 2)", "currency": "VARCHAR(10)",
    "add_time": "TIMESTAMPTZ", "update_time": "TIMESTAMPTZ",
    "org_id": "INTEGER", "org_name": "TEXT", "lost_reason": "TEXT", "visible_to": "TEXT",
    "close_time": "TIMESTAMPTZ", "won_time": "TIMESTAMPTZ", "lost_time": "TIMESTAMPTZ",
    "first_won_time": "TIMESTAMPTZ", "expected_close_date": "DATE", "probability": "NUMERIC(5,2)",
    "label": "TEXT"
}

# Define main table indexes
MAIN_TABLE_INDEXES = {
    "idx_pipedrive_data_update_time": "update_time DESC",
    "idx_pipedrive_data_stage_id": "stage_id",
    "idx_pipedrive_data_pipeline_id": "pipeline_id",
    "idx_pipedrive_data_person_id": "person_id",
    "idx_pipedrive_data_org_id": "org_id",
    "idx_pipedrive_data_owner_id": "owner_id",
    "idx_pipedrive_data_creator_user_id": "creator_user_id",
    "idx_pipedrive_data_status": "status",
    "idx_pipedrive_data_add_time": "add_time DESC",
    "idx_pipedrive_data_active_deals_update": "(update_time DESC) WHERE status NOT IN ('Ganho', 'Perdido', 'Deletado')"
}

class PipedriveDataRepository(DataRepositoryPort):
    """
    Repository implementation focused solely on the main 'pipedrive_data' table.
    Handles dynamic schema updates for custom fields and uses staging tables
    with COPY for efficient upserts. Leverages SchemaManager for DDL operations.
    """
    TABLE_NAME = "pipedrive_data"
    STAGING_TABLE_PREFIX = "staging_pipedrive_"
    STAGE_HISTORY_COLUMN_PREFIX = "moved_to_stage_" 

    def __init__(
        self,
        db_pool: pool.SimpleConnectionPool,
        schema_manager: SchemaManager,
        all_stages_details: List[Dict]
    ):
        self.db_pool = db_pool
        self.schema_manager = schema_manager
        self.log = log.bind(repository=self.__class__.__name__)
        self._all_stages_details = all_stages_details
        self._stage_history_columns_dict = self._prepare_stage_history_columns()
        self._stage_id_to_column_name_map = self._build_stage_id_to_col_map()
        self._cached_target_column_types: Optional[Dict[str, str]] = None
        
    def _refresh_column_type_cache(self) -> bool:
         """Fetches and updates the cached column types for the target table."""
         self.log.debug("Refreshing target column type cache", table_name=self.TABLE_NAME)
         try:
              types = self.schema_manager._get_table_column_types(self.TABLE_NAME)
              if not types:
                   self.log.error("Failed to fetch column types, cache not updated.")
                   self._cached_target_column_types = None 
                   return False
              self._cached_target_column_types = types
              self.log.info("Target column type cache refreshed", count=len(types))
              return True
         except Exception as e:
              self.log.error("Error refreshing column type cache", error=str(e), exc_info=True)
              self._cached_target_column_types = None
              return False

    def _build_stage_id_to_col_map(self) -> Dict[int, str]:
        """Builds the map of Stage ID -> Stage History Column Name."""
        id_map = {}
        for col_name in self._stage_history_columns_dict.keys():
             try:
                 parts = col_name.split('_')
                 if len(parts) > 1 and parts[-1].isdigit():
                      stage_id = int(parts[-1])
                      id_map[stage_id] = col_name
             except (ValueError, IndexError):
                 self.log.warning("Could not parse stage_id from history column name", column_name=col_name)
        return id_map

    def _prepare_stage_history_columns(self) -> Dict[str, str]:
        """Generates the dictionary {history_column_name: "TIMESTAMPTZ"}."""
        stage_history_cols = {}
        base_col_set = set(BASE_COLUMNS_DEFS.keys())
        processed_normalized_ids = set() 

        for stage in self._all_stages_details:
            stage_id = stage.get("id")
            stage_name = stage.get("name")
            if not stage_id or not stage_name: continue

            normalized = normalize_column_name(stage_name)
            if not normalized or normalized == "_invalid_normalized_name": continue

            final_col_name = f"{self.STAGE_HISTORY_COLUMN_PREFIX}{normalized}_{stage_id}"
            lookup_key = f"{normalized}_{stage_id}"

            if final_col_name not in base_col_set and lookup_key not in processed_normalized_ids:
                 stage_history_cols[final_col_name] = "TIMESTAMPTZ"
                 processed_normalized_ids.add(lookup_key)
            elif final_col_name in base_col_set:
                 self.log.error("Stage history column name conflicts with base column", col_name=final_col_name)

        return stage_history_cols

    def initialize_schema(self) -> None:
        """Initializes schema and populates the column type cache."""
        all_columns_with_types = {**BASE_COLUMNS_DEFS, **self._stage_history_columns_dict}
        self.schema_manager.ensure_table_exists(
            table_name=self.TABLE_NAME,
            column_definitions=list(all_columns_with_types.items()),
            primary_key="id",
            indexes=MAIN_TABLE_INDEXES
        )
        self._refresh_column_type_cache() 
        self.log.info("Schema initialized", table_name=self.TABLE_NAME)

    def _format_value_for_csv(self, value: Any) -> str:
         """Formats a Python value into a string suitable for COPY FROM STDIN."""
         if value is None or value is pd.NaT or (isinstance(value, float) and np.isnan(value)):
             return '\\N'
         elif isinstance(value, bool):
             return 't' if value else 'f'
         elif isinstance(value, datetime):
             if value.tzinfo is None:
                 dt_aware = value.replace(tzinfo=timezone.utc)
             else:
                 dt_aware = value.astimezone(timezone.utc)
             return dt_aware.isoformat(timespec='microseconds')
         elif isinstance(value, (date, time_obj, Decimal)):
             return str(value)
         elif isinstance(value, (dict, list, set, tuple)):
              try:
                 json_str = json.dumps(value)
                 return json_str.replace('\\', '\\\\').replace('|', '\\|').replace('\n', '\\n').replace('\r', '\\r')
              except TypeError:
                  self.log.warning("Could not JSON serialize value for CSV, using str()", value_type=type(value))
                  str_value = str(value)
         else:
             str_value = str(value)

         return str_value.replace('\\', '\\\\').replace('|', '\\|').replace('\n', '\\n').replace('\r', '\\r')


    def save_data_upsert(self, data: List[Dict[str, Any]]):
        """Saves data using staging table, COPY, dynamic columns, and cached types."""
        if not data:
            self.log.debug("No data provided to save_data_upsert, skipping.")
            return

        start_time = time.monotonic()
        record_count = len(data)
        log_ctx = self.log.bind(record_count=record_count, target_table=self.TABLE_NAME)

        all_keys_in_batch = set()
        for record in data:
            all_keys_in_batch.update(record.keys())
        if 'id' not in all_keys_in_batch:
             log_ctx.error("Input data for upsert is missing the 'id' key.")
             raise ValueError("Upsert data must contain the 'id' key.")
        final_columns_for_batch = sorted(list(all_keys_in_batch))

        try:
            schema_changed = self.schema_manager.ensure_columns_exist(self.TABLE_NAME, all_keys_in_batch)
            if schema_changed or self._cached_target_column_types is None:
                if not self._refresh_column_type_cache():
                     raise RuntimeError(f"Failed to refresh column types for {self.TABLE_NAME} after schema change.")
        except Exception as schema_update_err:
            log_ctx.error("Failed operation related to ensuring columns exist", columns=final_columns_for_batch, error=str(schema_update_err))
            raise

        target_column_types = self._cached_target_column_types
        if not target_column_types: 
             log_ctx.error("Target column type cache is not populated. Aborting UPSERT.")
             raise RuntimeError(f"Column type cache unavailable for table {self.TABLE_NAME}")

        conn = None
        staging_table_name = f"{self.STAGING_TABLE_PREFIX}{int(time.time())}_{random.randint(1000, 9999)}"
        staging_table_id = sql.Identifier(staging_table_name)
        target_table_id = sql.Identifier(self.TABLE_NAME)
        log_ctx = log_ctx.bind(staging_table=staging_table_name)

        try:
            conn = self.db_pool.getconn()
            with conn.cursor() as cur:
                staging_col_defs = [sql.SQL("{} TEXT").format(sql.Identifier(col)) for col in final_columns_for_batch]
                create_staging_sql = sql.SQL("CREATE UNLOGGED TABLE {staging_table} ({columns})").format(
                    staging_table=staging_table_id, columns=sql.SQL(', ').join(staging_col_defs)
                )
                log_ctx.debug("Creating dynamic staging table", columns=final_columns_for_batch)
                cur.execute(create_staging_sql)

                buffer = StringIO()
                copy_failed_records = 0
                try:
                    writer = csv.writer(buffer, delimiter='|', quoting=csv.QUOTE_MINIMAL, lineterminator='\n', escapechar='\\', doublequote=False)
                    for i, record in enumerate(data):
                        row = []
                        try:
                            for field in final_columns_for_batch:
                                value = record.get(field)
                                formatted_value = self._format_value_for_csv(value)
                                row.append(formatted_value)
                            writer.writerow(row)
                        except Exception as row_err:
                            copy_failed_records += 1
                            log_ctx.error("Error preparing record for COPY", record_index=i, error=str(row_err), record_id=record.get('id', 'N/A'), exc_info=True)
                    buffer.seek(0)

                    if copy_failed_records > 0:
                        log_ctx.warning("Some records failed CSV preparation", failed_count=copy_failed_records)

                    copy_sql = sql.SQL("COPY {staging_table} ({fields}) FROM STDIN WITH (FORMAT CSV, DELIMITER '|', NULL '\\N', ENCODING 'UTF8')").format(
                        staging_table=staging_table_id,
                        fields=sql.SQL(', ').join(map(sql.Identifier, final_columns_for_batch))
                    )
                    log_ctx.debug("Executing COPY to staging table.")
                    cur.copy_expert(copy_sql, buffer)
                    copy_row_count = cur.rowcount
                    log_ctx.debug("COPY command executed", copied_rows=copy_row_count, expected_rows=record_count - copy_failed_records)

                finally:
                    buffer.close()

                # 5. Execute UPSERT with dynamic columns and CASTING
                target_column_types = self.schema_manager._get_table_column_types(self.TABLE_NAME) 
                if not target_column_types:
                     log_ctx.error("Could not fetch target column types for casting. Aborting UPSERT.")
                     raise RuntimeError(f"Failed to get column types for table {self.TABLE_NAME}")

                insert_fields = sql.SQL(', ').join(map(sql.Identifier, final_columns_for_batch))
                update_assignments_list = []
                select_expressions = []

                for col in final_columns_for_batch:
                    # Build UPDATE SET assignments
                    if col != 'id': # Don't update the primary key
                         if col == 'add_time':
                             update_assignments_list.append(
                                 sql.SQL("{col} = COALESCE({target}.{col}, EXCLUDED.{col})").format(
                                     col=sql.Identifier(col), target=target_table_id
                                 )
                             )
                         else:
                             update_assignments_list.append(sql.SQL("{col} = EXCLUDED.{col}").format(col=sql.Identifier(col)))

                    # Build SELECT with CASTING logic
                    target_type = target_column_types.get(col, "TEXT").upper()
                    base_pg_type = target_type.split('(')[0] #

                    if base_pg_type in ('INTEGER', 'BIGINT', 'NUMERIC', 'DECIMAL', 'REAL', 'DOUBLE PRECISION', 'SMALLINT'):
                         # Try to cast numeric types, handle empty strings as NULL
                         select_expressions.append(
                             sql.SQL("NULLIF(TRIM({col}), '')::{type}").format(
                                 col=sql.Identifier(col), type=sql.SQL(base_pg_type)
                             )
                         )
                    elif base_pg_type in ('TIMESTAMP', 'TIMESTAMPTZ', 'DATE', 'TIME'):
                         # Try to cast date/time types, handle empty strings as NULL
                         select_expressions.append(
                             sql.SQL("NULLIF(TRIM({col}), '')::{type}").format(
                                 col=sql.Identifier(col), type=sql.SQL(base_pg_type)
                             )
                         )
                    elif base_pg_type == 'BOOLEAN':
                        # Handle 't'/'f' from COPY
                        select_expressions.append(
                            sql.SQL("CASE WHEN TRIM(LOWER({col})) = 't' THEN TRUE WHEN TRIM(LOWER({col})) = 'f' THEN FALSE ELSE NULL END").format(
                                col=sql.Identifier(col)
                            )
                        )
                    elif base_pg_type == 'JSONB':
                         # Try casting to JSONB, handle empty/invalid as NULL
                         select_expressions.append(
                             sql.SQL("(CASE WHEN TRIM({col}) = '' THEN NULL ELSE NULLIF(TRIM({col}), '')::JSONB END)").format(
                                col=sql.Identifier(col)
                             )
                         )
                    else: # TEXT, VARCHAR, etc.
                        # Trim whitespace but keep empty strings if they are meaningful
                        select_expressions.append(sql.SQL("TRIM({col})").format(col=sql.Identifier(col)))

                update_assignments = sql.SQL(', ').join(update_assignments_list)
                select_clause = sql.SQL(', ').join(select_expressions)

                upsert_sql = sql.SQL("""
                    INSERT INTO {target_table} ({insert_fields})
                    SELECT {select_clause} FROM {staging_table}
                    ON CONFLICT (id) DO UPDATE SET {update_assignments}
                    WHERE {target_table}.update_time IS NULL OR EXCLUDED.update_time >= {target_table}.update_time
                """).format(
                    target_table=target_table_id, insert_fields=insert_fields, select_clause=select_clause,
                    staging_table=staging_table_id, update_assignments=update_assignments
                )
                log_ctx.debug("Executing UPSERT from staging table.")
                cur.execute(upsert_sql)
                upserted_count = cur.rowcount
                conn.commit()
                duration = time.monotonic() - start_time
                log_ctx.info("Upsert completed successfully", affected_rows=upserted_count, duration_sec=f"{duration:.3f}s")

        except Exception as e:
            if conn: conn.rollback()
            log_ctx.error("Upsert failed", error=str(e), exc_info=True)
            raise 
        finally:
            # 6. Drop staging table and release connection
            if conn:
                try:
                    with conn.cursor() as final_cur:
                        drop_sql = sql.SQL("DROP TABLE IF EXISTS {staging_table}").format(staging_table=staging_table_id)
                        log_ctx.debug("Dropping staging table.")
                        final_cur.execute(drop_sql)
                    conn.commit() 
                except Exception as drop_err:
                    log_ctx.error("Failed to drop staging table", error=str(drop_err))
                finally:
                     self.db_pool.putconn(conn) 


    # --- Implementações dos outros métodos da interface ---
    def get_record_by_id(self, record_id: str) -> Optional[Dict]:
        """Fetches a single complete record by its ID."""
        conn = None
        log_ctx = self.log.bind(deal_id=record_id)
        try:
            conn = self.db_pool.getconn()
            with conn.cursor(cursor_factory=extras.DictCursor) as cur:
                # Select all columns currently in the table dynamically
                cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name = %s;", (self.TABLE_NAME,))
                columns = [row[0] for row in cur.fetchall()]
                if not columns:
                     log_ctx.error("Could not retrieve columns for table", table_name=self.TABLE_NAME)
                     return None

                select_cols_sql = sql.SQL(', ').join(map(sql.Identifier, columns))
                query = sql.SQL("SELECT {cols} FROM {table} WHERE id = %s").format(
                    cols=select_cols_sql,
                    table=sql.Identifier(self.TABLE_NAME)
                )
                cur.execute(query, (str(record_id),)) 
                row = cur.fetchone()
                log_ctx.debug("Record fetched by ID", found=row is not None)
                return dict(row) if row else None
        except Exception as e:
            log_ctx.error("Failed to get record by ID", error=str(e), exc_info=True)
            return None
        finally:
            if conn: self.db_pool.putconn(conn)

    def get_all_ids(self) -> Set[str]:
        """Returns a set of all existing deal IDs."""
        conn = None
        ids = set()
        try:
            conn = self.db_pool.getconn()
            with conn.cursor() as cur:
                cur.execute(sql.SQL("SELECT id FROM {table}").format(table=sql.Identifier(self.TABLE_NAME)))
                while True:
                    rows = cur.fetchmany(50000)
                    if not rows: break
                    ids.update(row[0] for row in rows)
                self.log.debug("Fetched all IDs", count=len(ids))
                return ids
        except Exception as e:
            self.log.error("Failed to get all IDs", error=str(e), exc_info=True)
            return set() 
        finally:
            if conn: self.db_pool.putconn(conn)

    def count_records(self) -> int:
        """Counts the total number of records in the main data table."""
        conn = None
        try:
            conn = self.db_pool.getconn()
            with conn.cursor() as cur:
                cur.execute(sql.SQL("SELECT COUNT(*) FROM {table}").format(table=sql.Identifier(self.TABLE_NAME)))
                count = cur.fetchone()[0]
                self.log.debug("Counted records", total=count)
                return count or 0
        except Exception as e:
            self.log.error("Failed to count records", error=str(e), exc_info=True)
            return -1 
        finally:
            if conn: self.db_pool.putconn(conn)

    def get_deals_needing_history_backfill(self, limit: int) -> List[str]:
        """Finds deal IDs potentially needing stage history backfill."""
        conn = None
        history_cols = list(self._stage_history_columns_dict.keys())
        if not history_cols:
            self.log.warning("No stage history columns defined, cannot find deals for backfill.")
            return []

        where_conditions = [sql.SQL("{} IS NULL").format(sql.Identifier(col)) for col in history_cols]
        where_clause = sql.SQL(" OR ").join(where_conditions)

        try:
            conn = self.db_pool.getconn()
            with conn.cursor() as cur:
                query = sql.SQL("""
                    SELECT id FROM {table}
                    WHERE {conditions}
                    ORDER BY add_time ASC NULLS FIRST, id -- Stable sort order
                    LIMIT %s
                """).format(
                    table=sql.Identifier(self.TABLE_NAME),
                    conditions=where_clause
                )
                cur.execute(query, (limit,))
                deal_ids = [row[0] for row in cur.fetchall()]
                self.log.info("Fetched deal IDs needing history backfill", count=len(deal_ids), limit=limit)
                return deal_ids
        except Exception as e:
            self.log.error("Failed to get deals for history backfill", error=str(e), exc_info=True)
            return []
        finally:
            if conn: self.db_pool.putconn(conn)

    def update_stage_history(self, updates: List[Dict[str, Any]]) -> None:
        """Applies stage history timestamp updates using UPDATE FROM VALUES."""
        if not updates:
            self.log.debug("No stage history updates to apply.")
            return

        conn = None
        start_time = time.monotonic()
        total_updated_count = 0
        valid_stage_columns = set(self._stage_history_columns_dict.keys())

        # Group updates by the column they affect for batching
        updates_by_column: Dict[str, List[Tuple[str, datetime]]] = {}
        for update in updates:
            deal_id = str(update.get('deal_id'))
            stage_column = update.get('stage_column')
            timestamp_val = update.get('timestamp') 

            if not deal_id or not stage_column or not isinstance(timestamp_val, datetime):
                 self.log.warning("Invalid data in stage history update", update_data=update)
                 continue
            if stage_column not in valid_stage_columns:
                 self.log.warning("Attempted update on non-existent history column", column=stage_column, deal_id=deal_id)
                 continue

            if stage_column not in updates_by_column:
                updates_by_column[stage_column] = []
            updates_by_column[stage_column].append((deal_id, timestamp_val))

        try:
            conn = self.db_pool.getconn()
            with conn.cursor() as cur:
                for stage_column, column_updates in updates_by_column.items():
                    if not column_updates: continue
                    column_id = sql.Identifier(stage_column)
                    table_id = sql.Identifier(self.TABLE_NAME)
                    values_tuples = [(upd[0], upd[1]) for upd in column_updates]

                    # Update only if the current value is NULL
                    update_sql = sql.SQL("""
                        UPDATE {table} AS t
                        SET {column_to_update} = v.ts
                        FROM (VALUES %s) AS v(id, ts)
                        WHERE t.id = v.id AND t.{column_to_update} IS NULL
                    """).format(
                        table=table_id,
                        column_to_update=column_id
                    )

                    try:
                        log_ctx = self.log.bind(stage_column=stage_column, batch_size=len(values_tuples))
                        log_ctx.debug("Executing stage history update batch")
                        extras.execute_values(cur, update_sql.as_string(cur), values_tuples, page_size=1000)
                        batch_updated_count = cur.rowcount
                        total_updated_count += batch_updated_count
                        log_ctx.debug("Stage history update batch executed", affected_rows=batch_updated_count)
                    except Exception as exec_err:
                        self.log.error("Failed executing batch update for stage history", stage_column=stage_column, error=str(exec_err), exc_info=True)
                        conn.rollback()
                        raise exec_err 

                conn.commit() 
                duration = time.monotonic() - start_time
                self.log.info(
                    "Stage history update run completed.",
                    total_updates_provided=len(updates),
                    total_rows_affected=total_updated_count,
                    columns_updated=list(updates_by_column.keys()),
                    duration_sec=f"{duration:.3f}s"
                )

        except Exception as e:
            if conn: conn.rollback() 
            self.log.error("Failed to update stage history", error=str(e), total_updates=len(updates), exc_info=True)
        finally:
            if conn:
                self.db_pool.putconn(conn)

    def count_deals_needing_backfill(self) -> int:
        """Counts how many deals potentially need backfill."""
        conn = None
        history_cols = list(self._stage_history_columns_dict.keys())
        if not history_cols:
            self.log.warning("No stage history columns defined, cannot count deals for backfill.")
            return -1

        where_conditions = [sql.SQL("{} IS NULL").format(sql.Identifier(col)) for col in history_cols]
        where_clause = sql.SQL(" OR ").join(where_conditions)

        try:
            conn = self.db_pool.getconn()
            with conn.cursor() as cur:
                query = sql.SQL("SELECT COUNT(*) FROM {table} WHERE {conditions}").format(
                    table=sql.Identifier(self.TABLE_NAME),
                    conditions=where_clause
                )
                cur.execute(query)
                count = cur.fetchone()[0]
                self.log.info("Counted deals needing history backfill", count=count)
                return count or 0
        except Exception as e:
            self.log.error("Failed to count deals for history backfill", error=str(e), exc_info=True)
            return -1
        finally:
            if conn: self.db_pool.putconn(conn)

    def validate_date_consistency(self) -> int:
         """Checks basic date consistency, returns number of issues."""
         conn = None
         try:
             conn = self.db_pool.getconn()
             with conn.cursor() as cur:
                 cur.execute(sql.SQL("""
                     SELECT COUNT(*) FROM {table}
                     WHERE add_time > NOW() -- Added in the future?
                       OR (update_time IS NOT NULL AND update_time < add_time)
                       OR (close_time IS NOT NULL AND close_time < add_time)
                       OR (won_time IS NOT NULL AND won_time < add_time)
                       OR (lost_time IS NOT NULL AND lost_time < add_time)
                 """).format(table=sql.Identifier(self.TABLE_NAME)))
                 count = cur.fetchone()[0]
                 if count > 0:
                      self.log.warning("Date consistency issues found", count=count)
                 return count or 0
         except Exception as e:
             self.log.error("Failed to validate date consistency", error=str(e), exc_info=True)
             return -1
         finally:
             if conn: self.db_pool.putconn(conn)