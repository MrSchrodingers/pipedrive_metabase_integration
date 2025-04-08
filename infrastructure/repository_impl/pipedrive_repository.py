from datetime import datetime, time
import time as py_time 
import json
import csv
from typing import Any, List, Dict, Optional, Set, Tuple
from io import StringIO
from psycopg2 import sql, extras
from psycopg2.extensions import cursor as DbCursor 
import structlog

from application.ports.data_repository_port import DataRepositoryPort
from application.utils.column_utils import normalize_column_name
from infrastructure.db_pool import DBConnectionPool

log = structlog.get_logger(__name__)

BASE_COLUMNS = [
    "id", "titulo", "creator_user_id", "creator_user_name", "person_id",
    "person_name", "stage_id", "stage_name", "pipeline_id", "pipeline_name",
    "owner_id", "owner_name", "status", "value", "currency",
    "add_time", "update_time",
]

COLUMN_TYPES = {
    "id": "TEXT PRIMARY KEY", "titulo": "TEXT", "creator_user_id": "INTEGER",
    "creator_user_name": "TEXT", "person_id": "INTEGER", "person_name": "TEXT",
    "stage_id": "INTEGER", "stage_name": "TEXT", "pipeline_id": "INTEGER",
    "pipeline_name": "TEXT", "owner_id": "INTEGER", "owner_name": "TEXT",
    "status": "TEXT", "value": "NUMERIC(18, 2)", "currency": "VARCHAR(10)",
    "add_time": "TIMESTAMPTZ", "update_time": "TIMESTAMPTZ",
}

class PipedriveRepository(DataRepositoryPort):
    TABLE_NAME = "pipedrive_data"
    STAGING_TABLE_PREFIX = "staging_pipedrive_"
    STAGE_HISTORY_COLUMN_PREFIX = "moved_to_stage_"
    SCHEMA_LOCK_ID = 47835

    def __init__(
        self,
        db_pool: DBConnectionPool, 
        custom_field_api_mapping: dict,
        all_stages_details: List[Dict]
        ):
        self.db_pool = db_pool
        self.log = log.bind(repository="PipedriveRepository")
        self._raw_custom_field_mapping = custom_field_api_mapping
        self._all_stages_details = all_stages_details 

        self._custom_columns_dict = self._prepare_custom_columns(custom_field_api_mapping)
        self._stage_history_columns_dict = self._prepare_stage_history_columns(all_stages_details)

        self.ensure_schema_exists()

    @property
    def custom_field_mapping(self) -> Dict[str, str]:
        """Returns the mapping of Pipedrive API Key -> Normalized Column Name."""
        return self._raw_custom_field_mapping

    def _prepare_custom_columns(self, api_mapping: Dict[str, str]) -> Dict[str, str]:
        """Prepares custom column names and types, ensuring no clashes with base columns."""
        custom_cols = {}
        base_col_set = set(BASE_COLUMNS)
        reserved_prefixes = (self.STAGE_HISTORY_COLUMN_PREFIX,)
        
        for api_key, normalized_name in api_mapping.items():
            if normalized_name in base_col_set:
                self.log.warning(
                    "Custom field normalized name clashes with base column, skipping.",
                    api_key=api_key, normalized_name=normalized_name
                )
                continue
            if any(normalized_name.startswith(prefix) for prefix in reserved_prefixes):
                 self.log.warning(
                    "Custom field normalized name clashes with reserved prefix, skipping.",
                    api_key=api_key, normalized_name=normalized_name
                )
                 continue

            custom_cols[normalized_name] = "TEXT"
        return custom_cols
    
    def _prepare_stage_history_columns(self, all_stages: List[Dict]) -> Dict[str, str]:
        """Prepares stage history column names (moved_to_stage_...) and types."""
        stage_history_cols = {}
        if not all_stages:
            self.log.warning("No stage details provided, cannot create stage history columns.")
            return {}

        base_col_set = set(BASE_COLUMNS)
        existing_custom_cols = set(self._custom_columns_dict.keys())
        processed_normalized_names = set()

        for stage in all_stages:
            stage_id = stage.get('id')
            stage_name = stage.get('name')
            if not stage_id or not stage_name:
                self.log.warning("Stage entry missing id or name", stage_data=stage)
                continue

            try:
                normalized_stage_name = normalize_column_name(stage_name)
                if not normalized_stage_name:
                    self.log.warning("Failed to normalize stage name", stage_id=stage_id, stage_name=stage_name)
                    continue

                column_name = f"{self.STAGE_HISTORY_COLUMN_PREFIX}{normalized_stage_name}"

                if column_name in base_col_set or column_name in existing_custom_cols:
                    self.log.error(
                        "CRITICAL: Normalized stage history column name clashes with base/custom column!",
                        stage_id=stage_id, stage_name=stage_name, column_name=column_name
                    )
                    continue

                if normalized_stage_name in processed_normalized_names:
                     self.log.warning(
                        "Duplicate normalized stage name detected. History column might overwrite.",
                        stage_id=stage_id, stage_name=stage_name, normalized_name=normalized_stage_name
                    )
                else:
                    stage_history_cols[column_name] = "TIMESTAMPTZ"
                    processed_normalized_names.add(normalized_stage_name)

            except Exception as e:
                self.log.error("Error processing stage for history column", stage_data=stage, error=str(e))

        self.log.info("Prepared stage history columns", count=len(stage_history_cols))
        return stage_history_cols

    def _get_all_columns(self) -> List[str]:
        """Returns a list of all base and active custom column names."""
        return BASE_COLUMNS + list(self._custom_columns_dict.keys()) + list(self._stage_history_columns_dict.keys())

    def _get_column_definitions(self) -> List[sql.SQL]:
        """Generates SQL column definitions for CREATE TABLE."""
        defs = []
        all_column_types = {**COLUMN_TYPES, **self._custom_columns_dict, **self._stage_history_columns_dict}
        ordered_cols = self._get_all_columns()

        for col in ordered_cols:
            col_type = all_column_types.get(col)
            if col_type:
                 defs.append(sql.SQL("{} {}").format(sql.Identifier(col), sql.SQL(col_type)))
            else:
                 self.log.error("Column definition missing type unexpectedly!", column_name=col)
                 defs.append(sql.SQL("{} TEXT").format(sql.Identifier(col)))
        return defs
                
    def ensure_schema_exists(self):
        """
        Ensures the target table and necessary indexes exist using an advisory lock
        to prevent race conditions during concurrent runs.
        """
        conn = None
        locked = False
        try:
            conn = self.db_pool.get_connection()
            with conn.cursor() as cur:
                cur.execute("SELECT pg_advisory_lock(%s)", (self.SCHEMA_LOCK_ID,))
                locked = True
                self.log.info("Acquired schema modification lock.", lock_id=self.SCHEMA_LOCK_ID)

                self._create_or_alter_table(cur)
                self._create_indexes(cur)
                conn.commit() 
                self.log.info("Schema check/modification committed.")
                
        except Exception as e:
            if conn: conn.rollback()
            self.log.error("Failed to ensure database schema", error=str(e), exc_info=True)
            raise
        finally:
            if conn and locked:
                try:
                    with conn.cursor() as unlock_cur:
                        unlock_cur.execute("SELECT pg_advisory_unlock(%s)", (self.SCHEMA_LOCK_ID,))
                    self.log.info("Released schema modification lock.", lock_id=self.SCHEMA_LOCK_ID)
                except Exception as unlock_err:
                    self.log.error("Failed to release schema lock", error=str(unlock_err))
            if conn:
                self.db_pool.release_connection(conn)

    def _create_or_alter_table(self, cur: DbCursor):
            """Creates the table or adds missing columns."""
            table_id = sql.Identifier(self.TABLE_NAME)
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_schema = 'public' AND table_name = %s
                );
            """, (self.TABLE_NAME,))
            table_exists = cur.fetchone()[0]
            
            all_target_columns_with_types = {
                **COLUMN_TYPES,
                **self._custom_columns_dict,
                **self._stage_history_columns_dict
            }
            all_target_column_names = self._get_all_columns() 

            if not table_exists:
                self.log.info("Table does not exist, creating.", table_name=self.TABLE_NAME)
                column_defs = self._get_column_definitions()
                if not column_defs:
                    raise RuntimeError("Cannot create table with no column definitions.")

                create_sql = sql.SQL("CREATE TABLE {table} ({columns})").format(
                    table=table_id,
                    columns=sql.SQL(',\n    ').join(column_defs)
                )
                self.log.debug("Executing CREATE TABLE SQL", sql_query=create_sql.as_string(cur))
                cur.execute(create_sql)
                self.log.info("Table created successfully.", table_name=self.TABLE_NAME)
            else:
                self.log.debug("Table exists, checking for missing columns.", table_name=self.TABLE_NAME)
                cur.execute("""
                    SELECT column_name FROM information_schema.columns
                    WHERE table_schema = 'public' AND table_name = %s;
                """, (self.TABLE_NAME,))
                existing_columns = {row[0] for row in cur.fetchall()}

                missing_columns = set(all_target_column_names) - existing_columns

                if missing_columns:
                    self.log.info("Adding missing columns to table.", missing=sorted(list(missing_columns)))
                    alter_statements = []
                    for col_name in sorted(list(missing_columns)):
                        col_type = all_target_columns_with_types.get(col_name)
                        if col_type:
                            alter_statements.append(sql.SQL("ADD COLUMN IF NOT EXISTS {} {}").format(
                                sql.Identifier(col_name),
                                sql.SQL(col_type)
                            ))
                        else:
                            self.log.error("Cannot add missing column, type definition not found.", column_name=col_name)

                    if alter_statements:
                        for stmt in alter_statements:
                            alter_sql = sql.SQL("ALTER TABLE {table} {add}").format(table=table_id, add=stmt)
                            try:
                                self.log.debug("Executing ALTER TABLE ADD COLUMN", sql_query=alter_sql.as_string(cur))
                                cur.execute(alter_sql)
                            except Exception as alter_err:
                                self.log.error("Failed to add column", column_name=stmt.strings[0], error=str(alter_err))
                                # Considerar se deve fazer rollback e parar
                                # conn.rollback() # Se uma coluna falhar, as outras podem depender dela?
                                # raise alter_err

                        self.log.info("Missing columns check/addition attempted.", count=len(missing_columns))
                else:
                    self.log.debug("No missing columns found.")

    def _create_indexes(self, cur: DbCursor):
        """Creates standard indexes if they don't exist."""
        table_id = sql.Identifier(self.TABLE_NAME)
        indexes = {
            "idx_pipedrive_update_time": sql.SQL("(update_time DESC)"),
            "idx_pipedrive_stage_id": sql.SQL("(stage_id)"), 
            "idx_pipedrive_pipeline_id": sql.SQL("(pipeline_id)"),
            "idx_pipedrive_status": sql.SQL("(status)"),
            "idx_pipedrive_add_time": sql.SQL("(add_time DESC)"),
            "idx_pipedrive_active_deals": sql.SQL("(update_time DESC) WHERE status NOT IN ('Ganho', 'Perdido', 'Deletado')")
        }
        
        important_normalized_stages = ['prospect', 'contact_made', 'proposal_sent']
        for norm_stage in important_normalized_stages:
           col_name = f"{self.STAGE_HISTORY_COLUMN_PREFIX}{norm_stage}"
           idx_name = f"idx_pipedrive_{norm_stage}_entry_time"
           cur.execute("SELECT 1 FROM information_schema.columns WHERE table_name=%s AND column_name=%s", (self.TABLE_NAME, col_name))
           if cur.fetchone():
              indexes[idx_name] = sql.SQL("({})").format(sql.Identifier(col_name))
           else:
              self.log.warning("Skipping index creation for non-existent stage history column", column_name=col_name)

        for idx_name, idx_definition in indexes.items():
            create_idx_sql = sql.SQL(
                "CREATE INDEX IF NOT EXISTS {} ON {} {}"
            ).format(sql.Identifier(idx_name), table_id, idx_definition)
            try:
                cur.execute(create_idx_sql)
                self.log.debug("Index checked/created.", index_name=idx_name)
            except Exception as idx_err:
                 self.log.warning("Failed to create index", index_name=idx_name, error=str(idx_err))

        self.log.debug("Index check/creation process completed.")

    def _record_to_csv_line(self, record: Dict, columns: List[str]) -> str:
        """Converts a dictionary record to a CSV string line for COPY."""
        output = StringIO()
        writer = csv.writer(output, delimiter='|', quoting=csv.QUOTE_MINIMAL, lineterminator='\n')
        row = []
        for field in columns:
            value = record.get(field)
            if value is None:
                row.append('\\N')
            elif isinstance(value, datetime):
                 row.append(value.isoformat())
            elif isinstance(value, bool):
                 row.append('t' if value else 'f')
            else:
                str_value = str(value)
                escaped_value = str_value.replace('|', '\\|').replace('\n', '\\n').replace('\r', '\\r').replace('\\', '\\\\')
                row.append(escaped_value)
                
        writer.writerow(row)
        csv_line = output.getvalue().strip('\n')
        output.close()
        return csv_line

    def save_data_upsert(self, data: List[Dict]):
        """
        Efficiently upserts data using a temporary staging table and COPY FROM STDIN.
        """
        if not data:
            self.log.debug("No data provided to save_data_upsert, skipping.")
            return

        conn = None
        start_time = py_time.monotonic()
        columns = self._get_all_columns() 
        staging_table_name = f"{self.STAGING_TABLE_PREFIX}{int(py_time.time())}_{abs(hash(tuple(data[0].keys())))}"
        staging_table_id = sql.Identifier(staging_table_name)
        target_table_id = sql.Identifier(self.TABLE_NAME)
        record_count = len(data)

        try:
            conn = self.db_pool.get_connection()
            with conn.cursor() as cur:
                # 1. Create Temporary Staging Table
                staging_col_defs = [sql.SQL("{} TEXT").format(sql.Identifier(col)) for col in columns]
                create_staging_sql = sql.SQL("""
                    CREATE UNLOGGED TABLE {staging_table} (
                        {columns}
                    ) ON COMMIT DROP;
                """).format(
                    staging_table=staging_table_id,
                    columns=sql.SQL(',\n').join(staging_col_defs)
                )
                cur.execute(create_staging_sql)
                self.log.debug("Created temporary staging table.", table_name=staging_table_name)

                # 2. Prepare data buffer for COPY
                buffer = StringIO()
                for record in data:
                    full_record = {col: record.get(col) for col in columns}
                    line = self._record_to_csv_line(full_record, columns)
                    buffer.write(line + '\n')
                buffer.seek(0)

                # 3. COPY data into Staging Table
                copy_sql = sql.SQL("COPY {staging_table} ({fields}) FROM STDIN WITH (FORMAT CSV, DELIMITER '|', NULL '\\N')").format(
                    staging_table=staging_table_id,
                    fields=sql.SQL(', ').join(map(sql.Identifier, columns))
                )
                self.log.debug("Executing COPY command.", table_name=staging_table_name)
                cur.copy_expert(copy_sql, buffer)
                self.log.debug("Copied data to staging table.", record_count=record_count, table_name=staging_table_name)

                # 4. Perform UPSERT from Staging to Target Table
                insert_fields = sql.SQL(', ').join(map(sql.Identifier, columns))
                update_columns = BASE_COLUMNS + list(self._custom_columns_dict.keys())
                update_assignments = sql.SQL(', ').join([
                    sql.SQL("{col} = EXCLUDED.{col}").format(col=sql.Identifier(col))
                    for col in update_columns if col != 'id'
                ])

                # Build SELECT clause with appropriate CASTs from TEXT staging table
                select_expressions = []
                target_types = {**COLUMN_TYPES, **self._custom_columns_dict, **self._stage_history_columns_dict}
                for col in columns:
                    full_type_definition = target_types.get(col, "TEXT")
                    base_pg_type = full_type_definition.split()[0].split('(')[0]
                    if base_pg_type.upper() not in ('TEXT', 'VARCHAR'):
                         select_expressions.append(
                             sql.SQL("CAST(NULLIF({col}, '') AS {type})").format(
                                 col=sql.Identifier(col),
                                 type=sql.SQL(base_pg_type)
                             )
                         )
                    else:
                         select_expressions.append(sql.SQL("{col}").format(col=sql.Identifier(col)))
                         
                select_clause = sql.SQL(', ').join(select_expressions)

                upsert_sql = sql.SQL("""
                    INSERT INTO {target_table} ({insert_fields})
                    SELECT {select_clause}
                    FROM {staging_table}
                    ON CONFLICT (id) DO UPDATE SET
                        {update_assignments}
x                """).format(
                    target_table=target_table_id,
                    insert_fields=insert_fields,
                    select_clause=select_clause,
                    staging_table=staging_table_id,
                    update_assignments=update_assignments
                )

                self.log.debug("Executing UPSERT command.", target_table=self.TABLE_NAME)
                cur.execute(upsert_sql)
                upserted_count = cur.rowcount
                conn.commit() 

                duration = py_time.monotonic() - start_time
                self.log.info(
                    "Upsert completed successfully.",
                    record_count=record_count,
                    affected_rows=upserted_count,
                    duration_sec=f"{duration:.3f}"
                )

        except Exception as e:
            if conn: conn.rollback()
            self.log.error("Upsert failed", error=str(e), record_count=record_count, exc_info=True)
            raise
        finally:
            if conn:
                self.db_pool.release_connection(conn)
                
    def get_deals_needing_history_backfill(self, limit: int = 10000) -> List[str]:
        """
        Busca IDs de deals que podem precisar de backfill histórico.
        Simplificação: Busca deals antigos onde *qualquer* coluna de stage history seja NULL.
        Uma lógica mais robusta poderia verificar quais colunas específicas estão faltando.
        """
        conn = None
        if not self._stage_history_columns_dict:
            self.log.warning("No stage history columns defined, cannot find deals for backfill.")
            return []

        where_conditions = [sql.SQL("{} IS NULL").format(sql.Identifier(col)) for col in self._stage_history_columns_dict.keys()]
        if not where_conditions:
             self.log.warning("Could not build WHERE clause for backfill query.")
             return []
        where_clause = sql.SQL(" OR ").join(where_conditions)

        try:
            conn = self.db_pool.get_connection()
            with conn.cursor() as cur:
                query = sql.SQL("""
                    SELECT id FROM {table}
                    WHERE {conditions}
                    ORDER BY add_time ASC
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
            if conn:
                self.db_pool.release_connection(conn)

    def update_stage_history(self, updates: List[Dict[str, Any]]):
        """
        Atualiza as colunas de histórico de stage para múltiplos deals.
        'updates' é uma lista de dicts: [{'deal_id': str, 'stage_column': str, 'timestamp': datetime}, ...]
        Usa UPDATE FROM VALUES para eficiência. Só atualiza se o valor atual for NULL.
        """
        if not updates:
            self.log.debug("No stage history updates to apply.")
            return

        conn = None
        start_time = py_time.monotonic()
        updated_count = 0

        updates_by_column: Dict[str, List[Tuple[str, datetime]]] = {}
        valid_stage_columns = set(self._stage_history_columns_dict.keys())

        for update in updates:
            deal_id = str(update.get('deal_id')) 
            stage_column = update.get('stage_column')
            timestamp = update.get('timestamp')

            if not deal_id or not stage_column or not isinstance(timestamp, datetime):
                 self.log.warning("Invalid data in stage history update", update_data=update)
                 continue

            if stage_column not in valid_stage_columns:
                 self.log.warning("Attempted to update non-existent/invalid stage history column", column_name=stage_column, deal_id=deal_id)
                 continue

            if stage_column not in updates_by_column:
                updates_by_column[stage_column] = []
            updates_by_column[stage_column].append((deal_id, timestamp))

        try:
            conn = self.db_pool.get_connection()
            with conn.cursor() as cur:
                for stage_column, column_updates in updates_by_column.items():
                    if not column_updates: continue

                    column_id = sql.Identifier(stage_column)
                    table_id = sql.Identifier(self.TABLE_NAME)

                    values_tuples = [(upd[0], upd[1]) for upd in column_updates]

                    update_sql = sql.SQL("""
                        UPDATE {table} AS t SET
                            {column_to_update} = v.ts
                        FROM (VALUES %s) AS v(id, ts)
                        WHERE t.id = v.id AND t.{column_to_update} IS NULL;
                    """).format(
                        table=table_id,
                        column_to_update=column_id
                    )

                    try:
                        extras.execute_values(cur, update_sql.as_string(cur), values_tuples)
                        updated_count += cur.rowcount
                        self.log.debug(f"Executed batch update for column '{stage_column}'", records_in_batch=len(values_tuples), affected_rows=cur.rowcount)
                    except Exception as exec_err:
                        self.log.error(f"Failed to execute batch update for column '{stage_column}'", error=str(exec_err), records_count=len(values_tuples), exc_info=True)
                        conn.rollback() 
                        raise exec_err 

                conn.commit()
                duration = time.monotonic() - start_time
                self.log.info(
                    "Stage history update batch completed.",
                    total_updates_processed=len(updates),
                    total_rows_affected=updated_count,
                    columns_updated=list(updates_by_column.keys()),
                    duration_sec=f"{duration:.3f}s"
                )

        except Exception as e:
            if conn: conn.rollback()
            self.log.error("Failed to update stage history", error=str(e), total_updates=len(updates), exc_info=True)
        finally:
            if conn:
                self.db_pool.release_connection(conn)

    def count_deals_needing_backfill(self) -> int:
        """
        Conta o número total de deals que precisam de backfill histórico
        (onde pelo menos uma coluna de histórico de stage é NULL).
        Retorna -1 em caso de erro.
        """
        conn = None
        if not self._stage_history_columns_dict:
            self.log.warning("No stage history columns defined, cannot count deals for backfill.")
            return -1

        where_conditions = [sql.SQL("{} IS NULL").format(sql.Identifier(col)) for col in self._stage_history_columns_dict.keys()]
        if not where_conditions:
             self.log.warning("Could not build WHERE clause for backfill count query.")
             return -1
        where_clause = sql.SQL(" OR ").join(where_conditions)

        try:
            conn = self.db_pool.get_connection()
            with conn.cursor() as cur:
                query = sql.SQL("""
                    SELECT COUNT(*) FROM {table}
                    WHERE {conditions}
                """).format(
                    table=sql.Identifier(self.TABLE_NAME),
                    conditions=where_clause
                )
                cur.execute(query)
                count = cur.fetchone()[0]
                self.log.info("Counted deals needing history backfill", count=count)
                return count if count is not None else 0
        except Exception as e:
            self.log.error("Failed to count deals for history backfill", error=str(e), exc_info=True)
            return -1 
        finally:
            if conn:
                self.db_pool.release_connection(conn)
                
    def filter_data_by_ids(self, data: List[Dict], id_key: str = "id") -> List[Dict]:
        """Filters data, returning records whose IDs are NOT in the database."""
        if not data:
            return []

        ids_to_check = {str(rec.get(id_key)) for rec in data if rec.get(id_key) is not None}
        if not ids_to_check:
             self.log.warning("No valid IDs found in data for filtering.")
             return data 

        conn = None
        try:
            conn = self.db_pool.get_connection()
            with conn.cursor() as cur:
                query = sql.SQL("SELECT id FROM {} WHERE id IN %s").format(sql.Identifier(self.TABLE_NAME))
                cur.execute(query, (tuple(ids_to_check),))
                existing_ids = {row[0] for row in cur.fetchall()}

            new_records = [rec for rec in data if str(rec.get(id_key)) not in existing_ids]
            self.log.debug("Filtered existing records.", initial_count=len(data), existing_count=len(existing_ids), new_count=len(new_records))
            return new_records

        except Exception as e:
            self.log.error("Failed to filter existing records by ID", error=str(e), exc_info=True)
            return data
        finally:
            if conn:
                self.db_pool.release_connection(conn)
                
    def count_records(self) -> int:
        """Conta o total de registros na tabela."""
        conn = self.db_pool.get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) FROM {self.TABLE_NAME}")
                return cur.fetchone()[0]
        finally:
            self.db_pool.release_connection(conn)

    def get_all_ids(self) -> Set[str]:
        """Retorna todos os IDs presentes no banco."""
        conn = self.db_pool.get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(f"SELECT id FROM {self.TABLE_NAME}")
                return {row[0] for row in cur.fetchall()}
        finally:
            self.db_pool.release_connection(conn)

    def get_record_by_id(self, record_id: str) -> Optional[Dict]:
        """Busca um registro completo pelo ID."""
        conn = self.db_pool.get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT * FROM {self.TABLE_NAME} WHERE id = %s",
                    (record_id,)
                )
                cols = [desc[0] for desc in cur.description]
                row = cur.fetchone()
                return dict(zip(cols, row)) if row else None
        finally:
            self.db_pool.release_connection(conn)

    def validate_date_consistency(self) -> int:
        """Verifica consistência básica de datas, retorna número de problemas."""
        conn = self.db_pool.get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(f"""
                    SELECT COUNT(*) FROM {self.TABLE_NAME}
                    WHERE add_time > CURRENT_DATE 
                        OR update_time < add_time
                        OR (close_time IS NOT NULL AND close_time < add_time)
                """)
                return cur.fetchone()[0]
        finally:
            self.db_pool.release_connection(conn)

    def save_configuration(self, key: str, value: Dict):
        """Salva configurações dinâmicas no banco."""
        conn = self.db_pool.get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO config (key, value) 
                    VALUES (%s, %s)
                    ON CONFLICT (key) DO UPDATE SET
                        value = EXCLUDED.value,
                        updated_at = NOW()
                """, (key, json.dumps(value)))
                conn.commit()
        finally:
            self.db_pool.release_connection(conn)

    def save_data(self, data: List[Dict]) -> None:
        """Default save implementation, uses upsert."""
        self.log.debug("Calling save_data, delegating to save_data_upsert.")
        self.save_data_upsert(data)