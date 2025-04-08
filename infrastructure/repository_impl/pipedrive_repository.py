from datetime import datetime, time, timezone # Adicionado timezone
import random
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
    "org_id", "org_name", "lost_reason", "visible_to", "close_time", "won_time", "lost_time",
    "first_won_time", "expected_close_date", "probability", "label",
    "owner_id", "owner_name"
]

COLUMN_TYPES = {
    "id": "TEXT PRIMARY KEY", "titulo": "TEXT", "creator_user_id": "INTEGER",
    "creator_user_name": "TEXT", "person_id": "INTEGER", "person_name": "TEXT",
    "stage_id": "INTEGER", "stage_name": "TEXT", "pipeline_id": "INTEGER",
    "pipeline_name": "TEXT", "owner_id": "INTEGER", "owner_name": "TEXT",
    "status": "TEXT", "value": "NUMERIC(18, 2)", "currency": "VARCHAR(10)",
    "add_time": "TIMESTAMPTZ", "update_time": "TIMESTAMPTZ",
    "org_id": "INTEGER", "org_name": "TEXT", "lost_reason": "TEXT", "visible_to": "TEXT", 
    "close_time": "TIMESTAMPTZ", "won_time": "TIMESTAMPTZ", "lost_time": "TIMESTAMPTZ",
    "first_won_time": "TIMESTAMPTZ", "expected_close_date": "DATE", "probability": "NUMERIC(5,2)",
    "label": "TEXT",
}


class PipedriveRepository(DataRepositoryPort):
    TABLE_NAME = "pipedrive_data"
    CONFIG_TABLE_NAME = "config" 
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

        self._stage_history_columns_dict = self._prepare_stage_history_columns(
            all_stages_details,
            existing_custom_cols=set(self._custom_columns_dict.keys()) 
        )

        self.ensure_schema_exists()

    @property
    def custom_field_mapping(self) -> Dict[str, str]:
        """Returns the mapping of Pipedrive API Key -> Normalized Column Name."""
        return self._raw_custom_field_mapping

    def _prepare_custom_columns(self, api_mapping: Dict[str, str]) -> Dict[str, str]:
        """Prepares custom column names and types, ensuring no clashes with base columns or prefixes."""
        custom_cols = {}
        base_col_set = set(BASE_COLUMNS)
        reserved_prefixes = (self.STAGE_HISTORY_COLUMN_PREFIX,)

        for api_key, normalized_name in api_mapping.items():
            if not normalized_name or normalized_name == "_invalid_normalized_name":
                self.log.warning("Invalid or empty normalized name for custom field, skipping.",
                                 api_key=api_key, normalized_name=normalized_name)
                continue

            if normalized_name in base_col_set:
                self.log.warning(
                    "Custom field normalized name clashes with base column, skipping.",
                    api_key=api_key, normalized_name=normalized_name
                )
                continue
            if any(normalized_name.startswith(prefix) for prefix in reserved_prefixes):
                 self.log.warning(
                     "Custom field normalized name uses a reserved prefix, skipping.",
                     api_key=api_key, normalized_name=normalized_name
                 )
                 continue

            custom_cols[normalized_name] = "TEXT"
        self.log.info("Prepared custom columns", count=len(custom_cols))
        return custom_cols

    def _prepare_stage_history_columns(self, all_stages: List[Dict], existing_custom_cols: Set[str]) -> Dict[str, str]:
        """
        Prepares stage history column names (moved_to_stage_...) and types,
        handling duplicate normalized names by adding suffixes (_2, _3...).
        """
        stage_history_cols = {}
        if not all_stages:
            self.log.warning("No stage details provided, cannot create stage history columns.")
            return {}

        base_col_set = set(BASE_COLUMNS)
        forbidden_column_names = base_col_set.union(existing_custom_cols)

        normalized_name_counts: Dict[str, int] = {}

        for stage in all_stages:
            stage_id = stage.get('id')
            stage_name = stage.get('name')
            if not stage_id or not stage_name:
                self.log.warning("Stage entry missing id or name", stage_data=stage)
                continue

            try:
                normalized_stage_name = normalize_column_name(stage_name)
                if not normalized_stage_name or normalized_stage_name == "_invalid_normalized_name":
                    self.log.warning("Failed to normalize stage name", stage_id=stage_id, stage_name=stage_name)
                    continue

                base_column_name = f"{self.STAGE_HISTORY_COLUMN_PREFIX}{normalized_stage_name}"
                final_column_name = base_column_name

                count = normalized_name_counts.get(normalized_stage_name, 0)
                if count > 0:
                    suffix = count + 1
                    final_column_name = f"{base_column_name}_{suffix}"
                    self.log.warning(
                        "Duplicate normalized stage name detected. Using suffixed column name.",
                        stage_id=stage_id, stage_name=stage_name,
                        normalized_name=normalized_stage_name,
                        original_column=base_column_name,
                        final_column=final_column_name
                    )

                normalized_name_counts[normalized_stage_name] = count + 1

                if final_column_name in forbidden_column_names:
                    self.log.error(
                        "CRITICAL: Final stage history column name clashes with base/custom column! Skipping.",
                        stage_id=stage_id, stage_name=stage_name,
                        normalized_name=normalized_stage_name,
                        final_column=final_column_name
                    )
                    normalized_name_counts[normalized_stage_name] -= 1
                    continue

                stage_history_cols[final_column_name] = "TIMESTAMPTZ"

            except Exception as e:
                self.log.error("Error processing stage for history column", stage_data=stage, error=str(e))

        self.log.info("Prepared stage history columns", count=len(stage_history_cols))
        return stage_history_cols


    def _get_all_columns(self) -> List[str]:
        """Returns a list of all base, custom, and stage history column names."""
        return BASE_COLUMNS + sorted(list(self._custom_columns_dict.keys())) + sorted(list(self._stage_history_columns_dict.keys()))

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
        """Ensures the target tables (data and config) and necessary indexes exist."""
        conn = None
        locked = False
        try:
            conn = self.db_pool.get_connection()
            with conn.cursor() as cur:
                cur.execute("SELECT pg_advisory_lock(%s)", (self.SCHEMA_LOCK_ID,))
                locked = True
                self.log.info("Acquired schema modification lock.", lock_id=self.SCHEMA_LOCK_ID)

                self._create_or_alter_table(cur, self.TABLE_NAME, self._get_column_definitions)
                self._create_or_alter_table(cur, self.CONFIG_TABLE_NAME, self._get_config_column_definitions)

                self._create_indexes(cur)

                conn.commit()
                self.log.info("Schema check/modification committed.")

        except Exception as e:
            if conn: conn.rollback()
            self.log.error("Failed to ensure database schema", error=str(e), exc_info=True)
            raise
        finally:
            if conn:
                if locked:
                    try:
                        with conn.cursor() as unlock_cur:
                             unlock_cur.execute("SELECT pg_advisory_unlock(%s)", (self.SCHEMA_LOCK_ID,))
                        self.log.info("Released schema modification lock.", lock_id=self.SCHEMA_LOCK_ID)
                    except Exception as unlock_err:
                        self.log.error("Failed to release schema lock", error=str(unlock_err))
                self.db_pool.release_connection(conn)

    def _get_config_column_definitions(self) -> List[sql.SQL]:
        """Returns column definitions specifically for the config table."""
        return [
            sql.SQL("key TEXT PRIMARY KEY"),
            sql.SQL("value JSONB"),
            sql.SQL("updated_at TIMESTAMPTZ")
        ]

    def _create_or_alter_table(self, cur: DbCursor, table_name: str, get_col_defs_func: callable):
        """Generic function to create a table or add missing columns."""
        table_id = sql.Identifier(table_name)
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_schema = 'public' AND table_name = %s
            );
        """, (table_name,))
        table_exists = cur.fetchone()[0]

        column_defs_sql = get_col_defs_func()
        expected_columns_with_types = {}
        for col_def in column_defs_sql:
            parts = col_def.strings
            if len(parts) >= 2:
                 col_name = parts[0]
                 col_type = parts[1].strip() 
                 expected_columns_with_types[col_name] = col_type
            else:
                 self.log.error("Could not parse column definition", definition=col_def.as_string(cur))

        expected_column_names = list(expected_columns_with_types.keys())

        if not table_exists:
            self.log.info("Table does not exist, creating.", table_name=table_name)
            if not column_defs_sql:
                raise RuntimeError(f"Cannot create table '{table_name}' with no column definitions.")

            create_sql = sql.SQL("CREATE TABLE IF NOT EXISTS {table} ({columns})").format( 
                table=table_id,
                columns=sql.SQL(',\n    ').join(column_defs_sql)
            )
            self.log.debug(f"Executing CREATE TABLE for {table_name}", sql_query=create_sql.as_string(cur))
            cur.execute(create_sql)
            self.log.info("Table created successfully.", table_name=table_name)
        else:
            self.log.debug("Table exists, checking for missing columns.", table_name=table_name)
            cur.execute("""
                SELECT column_name FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = %s;
            """, (table_name,))
            existing_columns = {row[0] for row in cur.fetchall()}

            missing_columns = set(expected_column_names) - existing_columns

            if missing_columns:
                self.log.info(f"Adding missing columns to table '{table_name}'.", missing=sorted(list(missing_columns)))
                alter_statements = []
                for col_name in sorted(list(missing_columns)):
                    col_type = expected_columns_with_types.get(col_name)
                    if col_type:
                        alter_statements.append(sql.SQL("ADD COLUMN IF NOT EXISTS {} {}").format(
                            sql.Identifier(col_name),
                            sql.SQL(col_type)
                        ))
                    else:
                        self.log.error(f"Cannot add missing column to '{table_name}', type definition not found.", column_name=col_name)

                if alter_statements:
                    for stmt in alter_statements:
                        alter_sql = sql.SQL("ALTER TABLE {table} {add}").format(table=table_id, add=stmt)
                        try:
                            col_name_being_added = stmt.strings[0]
                            self.log.debug(f"Executing ALTER TABLE ADD COLUMN on {table_name}", column_name=col_name_being_added)
                            cur.execute(alter_sql)
                        except Exception as alter_err:
                            self.log.error(f"Failed to add column to {table_name}", column_name=col_name_being_added, error=str(alter_err))
                self.log.info(f"Missing columns check/addition attempted for {table_name}.", count=len(missing_columns))
            else:
                self.log.debug(f"No missing columns found for {table_name}.")


    def _create_indexes(self, cur: DbCursor):
        """Creates standard indexes if they don't exist on the main data table."""
        table_id = sql.Identifier(self.TABLE_NAME)
        indexes_to_create = {
            f"idx_{self.TABLE_NAME}_update_time": sql.SQL("(update_time DESC)"),
            f"idx_{self.TABLE_NAME}_stage_id": sql.SQL("(stage_id)"),
            f"idx_{self.TABLE_NAME}_pipeline_id": sql.SQL("(pipeline_id)"),
            f"idx_{self.TABLE_NAME}_status": sql.SQL("(status)"),
            f"idx_{self.TABLE_NAME}_add_time": sql.SQL("(add_time DESC)"),
            f"idx_{self.TABLE_NAME}_active_deals_update": sql.SQL("(update_time DESC) WHERE status NOT IN ('Ganho', 'Perdido', 'Deletado')")
        }

        existing_history_columns = self._stage_history_columns_dict.keys()
        important_normalized_stages = ['prospect', 'contact_made', 'proposal_sent']
        for history_col_name in existing_history_columns:
             norm_stage_part = history_col_name.replace(self.STAGE_HISTORY_COLUMN_PREFIX, '')
             if True:
                 idx_name = f"idx_{self.TABLE_NAME}_{norm_stage_part}_entry"
                 indexes_to_create[idx_name] = sql.SQL("({})").format(sql.Identifier(history_col_name))


        for idx_name, idx_definition in indexes_to_create.items():
            cur.execute("SELECT 1 FROM pg_class WHERE relname = %s AND relkind = 'i'", (idx_name,))
            index_exists = cur.fetchone()

            if not index_exists:
                create_idx_sql = sql.SQL("CREATE INDEX {} ON {} {}").format(
                    sql.Identifier(idx_name),
                    table_id,
                    idx_definition
                )
                try:
                    self.log.debug("Creating index.", index_name=idx_name)
                    cur.execute(create_idx_sql)
                except Exception as idx_err:
                    self.log.warning("Failed to create index", index_name=idx_name, error=str(idx_err))
            else:
                 self.log.debug("Index already exists.", index_name=idx_name)

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
        """Efficiently upserts data using an UNLOGGED staging table and COPY."""
        if not data:
            self.log.debug("No data provided to save_data_upsert, skipping.")
            return

        conn = None
        start_time = py_time.monotonic()
        columns = self._get_all_columns() 
        unique_suffix = f"{int(py_time.time())}_{random.randint(1000, 9999)}"
        staging_table_name = f"{self.STAGING_TABLE_PREFIX}{unique_suffix}"
        staging_table_id = sql.Identifier(staging_table_name)
        target_table_id = sql.Identifier(self.TABLE_NAME)
        record_count = len(data)
        self.log.debug("Starting upsert process", record_count=record_count, column_count=len(columns), target_table=self.TABLE_NAME)

        try:
            conn = self.db_pool.get_connection()
            with conn.cursor() as cur:
                staging_col_defs = [sql.SQL("{} TEXT").format(sql.Identifier(col)) for col in columns]
                create_staging_sql = sql.SQL("""
                    CREATE UNLOGGED TABLE {staging_table} (
                        {columns}
                    )
                """).format(
                    staging_table=staging_table_id,
                    columns=sql.SQL(',\n    ').join(staging_col_defs)
                )
                self.log.debug("Creating unlogged staging table.", table_name=staging_table_name)
                cur.execute(create_staging_sql)

                # 2. Prepare data buffer for COPY
                buffer = StringIO()
                try: 
                    for i, record in enumerate(data):
                        full_record = {col: record.get(col) for col in columns}
                        try:
                            line = self._record_to_csv_line(full_record, columns)
                            buffer.write(line + '\n')
                        except Exception as csv_err:
                            self.log.error("Error converting record to CSV line", record_index=i, record_preview=str(full_record)[:100], error=str(csv_err))
                    buffer.seek(0)

                    # 3. COPY data into Staging Table
                    copy_sql = sql.SQL("COPY {staging_table} ({fields}) FROM STDIN WITH (FORMAT CSV, DELIMITER '|', NULL '\\N', ENCODING 'UTF8')").format(
                        staging_table=staging_table_id,
                        fields=sql.SQL(', ').join(map(sql.Identifier, columns))
                    )
                    self.log.debug("Executing COPY command.", table_name=staging_table_name)
                    cur.copy_expert(copy_sql, buffer)
                    copy_row_count = cur.rowcount
                    self.log.debug("Copied data to staging table.", copied_row_count=copy_row_count, expected_count=record_count, table_name=staging_table_name)
                    if copy_row_count != record_count:
                         self.log.warning("COPY command copied unexpected number of rows!", expected=record_count, actual=copy_row_count)

                finally:
                    buffer.close()

                # 4. Perform UPSERT from Staging to Target Table with CASTING
                insert_fields = sql.SQL(', ').join(map(sql.Identifier, columns))
                update_columns = [col for col in columns if col != 'id' and col != 'add_time'] 
                update_assignments = sql.SQL(', ').join([
                    sql.SQL("{col} = EXCLUDED.{col}").format(col=sql.Identifier(col))
                    for col in update_columns
                ])

                select_expressions = []
                target_types = {**COLUMN_TYPES, **self._custom_columns_dict, **self._stage_history_columns_dict}
                for col in columns:
                    full_type_definition = target_types.get(col, "TEXT")
                    base_pg_type = full_type_definition.split()[0].split('(')[0].upper()

                    if col == 'id':
                         select_expressions.append(sql.Identifier(col))
                    elif base_pg_type in ('INTEGER', 'BIGINT', 'NUMERIC', 'DECIMAL', 'REAL', 'DOUBLE', 'DATE', 'TIMESTAMP', 'TIMESTAMPTZ', 'BOOLEAN'):
                         select_expressions.append(
                             sql.SQL("NULLIF({col}, '')::{type}").format(
                                 col=sql.Identifier(col),
                                 type=sql.SQL(base_pg_type)
                             )
                         )
                    else:
                         select_expressions.append(sql.Identifier(col))

                select_clause = sql.SQL(', ').join(select_expressions)

                upsert_sql = sql.SQL("""
                    INSERT INTO {target_table} ({insert_fields})
                    SELECT {select_clause}
                    FROM {staging_table}
                    ON CONFLICT (id) DO UPDATE SET
                        {update_assignments}
                    -- Opcional: Adicionar WHERE para otimizar updates desnecessários
                    -- WHERE t.update_time IS DISTINCT FROM EXCLUDED.update_time -- Exemplo
                """).format(
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
                self.log.debug("Commit successful after UPSERT.")

                # 5. Drop Staging Table (SEMPRE fazer isso no finally)

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
                try:
                    with conn.cursor() as final_cur:
                         drop_sql = sql.SQL("DROP TABLE IF EXISTS {staging_table}").format(staging_table=staging_table_id)
                         self.log.debug("Dropping staging table.", table_name=staging_table_name)
                         final_cur.execute(drop_sql)
                         conn.commit()
                except Exception as drop_err:
                     self.log.error("Failed to drop staging table", table_name=staging_table_name, error=str(drop_err))
                finally:
                    self.db_pool.release_connection(conn)


    # --- Funções de Leitura e Validação ---

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

    def get_record_by_id(self, record_id: Any) -> Optional[Dict]:
        """Busca um registro completo pelo ID, tratando ID como TEXT."""
        conn = None
        record_id_str = str(record_id)
        self.log.debug("Fetching record by ID", record_id=record_id_str)
        try:
            conn = self.db_pool.get_connection()
            with conn.cursor(cursor_factory=extras.DictCursor) as cur:
                query = sql.SQL("SELECT * FROM {table} WHERE id = %s").format(
                    table=sql.Identifier(self.TABLE_NAME)
                )
                cur.execute(query, (record_id_str,))
                row = cur.fetchone()
                return dict(row) if row else None
        except Exception as e:
             self.log.error("Failed to get record by ID", record_id=record_id_str, error=str(e), exc_info=True)
             return None
        finally:
            if conn:
                self.db_pool.release_connection(conn)



    def save_configuration(self, key: str, value: Dict):
        """Salva configurações dinâmicas (formato JSONB) no banco."""
        conn = None
        config_table_id = sql.Identifier(self.CONFIG_TABLE_NAME)
        self.log.debug("Saving configuration", config_key=key)
        try:
            conn = self.db_pool.get_connection()
            with conn.cursor() as cur:
                upsert_sql = sql.SQL("""
                    INSERT INTO {config_table} (key, value, updated_at)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (key) DO UPDATE SET
                        value = EXCLUDED.value,
                        updated_at = EXCLUDED.updated_at;
                """).format(config_table=config_table_id)

                timestamp_str = value.get('updated_at', datetime.now(timezone.utc).isoformat())
                try:
                    ts_obj = datetime.fromisoformat(timestamp_str).astimezone(timezone.utc)
                except Exception:
                    self.log.warning("Could not parse timestamp from config value, using current time.", config_key=key, value_ts=timestamp_str)
                    ts_obj = datetime.now(timezone.utc)

                cur.execute(upsert_sql, (key, json.dumps(value), ts_obj)) 
                conn.commit()
                self.log.info("Configuration saved successfully", config_key=key)
        except Exception as e:
            if conn: conn.rollback()
            self.log.error("Failed to save configuration", config_key=key, error=str(e), exc_info=True)
        finally:
            if conn:
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

    def count_records(self) -> int:
        """Conta o total de registros na tabela."""
        conn = self.db_pool.get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) FROM {self.TABLE_NAME}")
                return cur.fetchone()[0]
        finally:
            self.db_pool.release_connection(conn)
            
    def save_data(self, data: List[Dict]) -> None:
        """Default save implementation, uses upsert."""
        self.log.debug("Calling save_data, delegating to save_data_upsert.")
        self.save_data_upsert(data)