from datetime import datetime
import time as py_time 
import json
import csv
from typing import List, Dict
from io import StringIO
from psycopg2 import sql
from psycopg2.extensions import cursor as DbCursor 
import structlog

from application.ports.data_repository_port import DataRepositoryPort
from application.utils.replace_nan_with_none_recursive import replace_nan_with_none_recursive
from infrastructure.monitoring.metrics import db_operation_duration_hist
from infrastructure.db_pool import DBConnectionPool

log = structlog.get_logger(__name__)

BASE_COLUMNS = [
    "id",
    "titulo",
    "creator_user_id",
    "creator_user_name",
    "person_id",
    "person_name",  
    "stage_id",
    "stage_name",
    "pipeline_id",
    "pipeline_name",
    "owner_id", 
    "owner_name", 
    "status",
    "value",
    "currency",
    "add_time",
    "update_time",
]

COLUMN_TYPES = {
    "id": "TEXT PRIMARY KEY",
    "titulo": "TEXT",
    "creator_user_id": "INTEGER",
    "creator_user_name": "TEXT",
    "person_id": "INTEGER",
    "person_name": "TEXT", 
    "person_info": "JSONB",
    "stage_id": "INTEGER",
    "stage_name": "TEXT",
    "pipeline_id": "INTEGER",
    "pipeline_name": "TEXT",
    "owner_id": "INTEGER",
    "owner_name": "TEXT",
    "status": "TEXT",
    "value": "NUMERIC(18, 2)",
    "currency": "VARCHAR(10)",
    "add_time": "TIMESTAMPTZ",
    "update_time": "TIMESTAMPTZ",
}

JSONB_FIELDS = {'creator_user', 'user_info', 'person_info'}

class PipedriveRepository(DataRepositoryPort):
    TABLE_NAME = "pipedrive_data"
    STAGING_TABLE_PREFIX = "staging_pipedrive_"
    SCHEMA_LOCK_ID = 47835

    def __init__(self, db_pool: DBConnectionPool, custom_field_api_mapping: dict):
        self.db_pool = db_pool
        self.log = log.bind(repository="PipedriveRepository")
        self._raw_custom_field_mapping = custom_field_api_mapping
        self._custom_columns_dict = self._prepare_custom_columns(custom_field_api_mapping)
        self.ensure_schema_exists()

    @property
    def custom_field_mapping(self) -> Dict[str, str]:
        """Returns the mapping of Pipedrive API Key -> Normalized Column Name."""
        return self._raw_custom_field_mapping

    def _prepare_custom_columns(self, api_mapping: Dict[str, str]) -> Dict[str, str]:
        """Prepares custom column names and types, ensuring no clashes with base columns."""
        custom_cols = {}
        base_col_set = set(BASE_COLUMNS)
        for api_key, normalized_name in api_mapping.items():
            if normalized_name in base_col_set:
                self.log.warning(
                    "Custom field normalized name clashes with base column, skipping.",
                    api_key=api_key, normalized_name=normalized_name
                )
                continue
            custom_cols[normalized_name] = "TEXT"
        return custom_cols

    def _get_all_columns(self) -> List[str]:
        """Returns a list of all base and active custom column names."""
        return BASE_COLUMNS + list(self._custom_columns_dict.keys())

    def _get_column_definitions(self) -> List[sql.SQL]:
        """Generates SQL column definitions for CREATE TABLE."""
        defs = []
        # Base columns
        for col in BASE_COLUMNS:
            col_type = COLUMN_TYPES.get(col, "TEXT")
            defs.append(sql.SQL("{} {}").format(sql.Identifier(col), sql.SQL(col_type)))
        # Custom columns
        for col_name, col_type in self._custom_columns_dict.items():
            defs.append(sql.SQL("{} {}").format(sql.Identifier(col_name), sql.SQL(col_type)))
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
        except Exception as e:
            if conn: conn.rollback()
            self.log.error("Failed to ensure database schema", error=str(e), exc_info=True)
            raise
        finally:
            if conn and locked:
                try:
                    with conn.cursor() as cur:
                        cur.execute("SELECT pg_advisory_unlock(%s)", (self.SCHEMA_LOCK_ID,))
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

            if not table_exists:
                self.log.info("Table does not exist, creating.", table_name=self.TABLE_NAME)
                column_defs = self._get_column_definitions()

                create_sql_base = sql.SQL("""
                    CREATE TABLE {table} (
                        {columns}
                    )
                """)

                create_sql = create_sql_base.format(
                    table=table_id,
                    columns=sql.SQL(',\n    ').join(column_defs)
                )

                self.log.debug("Executing CREATE TABLE SQL", sql_query=create_sql.as_string(cur))
                cur.execute(create_sql)
                self.log.info("Table created successfully.", table_name=self.TABLE_NAME)
            else:
                self.log.debug("Table already exists, checking for missing columns.", table_name=self.TABLE_NAME)
                cur.execute("""
                    SELECT column_name FROM information_schema.columns
                    WHERE table_schema = 'public' AND table_name = %s;
                """, (self.TABLE_NAME,))
                existing_columns = {row[0] for row in cur.fetchall()}

                all_target_columns = self._get_all_columns()
                missing_columns = set(all_target_columns) - existing_columns

                if missing_columns:
                    self.log.info("Adding missing columns to table.", missing=list(missing_columns))
                    alter_statements = []
                    for col in missing_columns.intersection(BASE_COLUMNS):
                        col_type = COLUMN_TYPES.get(col, "TEXT")
                        alter_statements.append(sql.SQL("ADD COLUMN IF NOT EXISTS {} {}").format(sql.Identifier(col), sql.SQL(col_type)))
                    for col in missing_columns.intersection(self._custom_columns_dict.keys()):
                        col_type = self._custom_columns_dict.get(col, "TEXT")
                        alter_statements.append(sql.SQL("ADD COLUMN IF NOT EXISTS {} {}").format(sql.Identifier(col), sql.SQL(col_type)))

                    if alter_statements:
                        alter_sql = sql.SQL("ALTER TABLE {table} {adds}").format(
                            table=table_id,
                            adds=sql.SQL(', ').join(alter_statements)
                        )
                        self.log.debug("Executing ALTER TABLE SQL", sql_query=alter_sql.as_string(cur))
                        cur.execute(alter_sql)
                        self.log.info("Missing columns check/addition complete.", count=len(missing_columns))
                else:
                    self.log.debug("No missing columns found.")

    def _create_indexes(self, cur: DbCursor):
        """Creates standard indexes if they don't exist."""
        table_id = sql.Identifier(self.TABLE_NAME)
        indexes = {
            "idx_pipedrive_update_time": sql.SQL("(update_time DESC)"),
            "idx_pipedrive_stage_name": sql.SQL("(stage_name)"),
            "idx_pipedrive_pipeline_id": sql.SQL("(pipeline_id)"),
            "idx_pipedrive_status": sql.SQL("(status)"),

            "idx_pipedrive_active_deals": sql.SQL("(update_time DESC) WHERE status NOT IN ('Ganho', 'Perdido', 'Em aberto', 'Deletado')")
        }

        for idx_name, idx_definition in indexes.items():
            create_idx_sql = sql.SQL(
                "CREATE INDEX IF NOT EXISTS {} ON {} {}"
            ).format(sql.Identifier(idx_name), table_id, idx_definition)
            try:
                cur.execute(create_idx_sql)
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
                row.append('')
            elif field in JSONB_FIELDS:
                try:
                    cleaned_value = replace_nan_with_none_recursive(value)
                    json_str = json.dumps(cleaned_value, ensure_ascii=False, separators=(',', ':'), allow_nan=False)
                    row.append(json_str)
                except (TypeError, ValueError) as json_err:
                    self.log.warning(
                    "JSON serialization failed for field (ensure NaN/Inf are replaced with None)",
                    field=field, record_id=record.get("id"), error=str(json_err), value_preview=str(value)[:100]
                    )
                    row.append('')
            elif isinstance(value, datetime):
                 row.append(value.isoformat())
            elif isinstance(value, bool):
                 row.append('t' if value else 'f')
            else:
                row.append(str(value))
        writer.writerow(row)
        return output.getvalue().strip() 

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
                    CREATE TEMPORARY TABLE {staging_table} (
                        {columns}
                    ) ON COMMIT DROP; -- Drop automatically on commit/rollback
                """).format(
                    staging_table=staging_table_id,
                    columns=sql.SQL(',\n').join(staging_col_defs)
                )
                cur.execute(create_staging_sql)
                self.log.debug("Created temporary staging table.", table_name=staging_table_name)

                # 2. Prepare data buffer for COPY
                buffer = StringIO()
                for record in data:
                    line = self._record_to_csv_line(record, columns)
                    buffer.write(line + '\n')
                buffer.seek(0)

                # 3. COPY data into Staging Table
                copy_sql = sql.SQL("COPY {staging_table} ({fields}) FROM STDIN WITH (FORMAT CSV, DELIMITER '|', NULL '')").format(
                    staging_table=staging_table_id,
                    fields=sql.SQL(', ').join(map(sql.Identifier, columns))
                )
                cur.copy_expert(copy_sql, buffer)
                self.log.debug("Copied data to staging table.", record_count=record_count, table_name=staging_table_name)

                # 4. Perform UPSERT from Staging to Target Table
                insert_fields = sql.SQL(', ').join(map(sql.Identifier, columns))
                update_assignments = sql.SQL(', ').join([
                    sql.SQL("{col} = EXCLUDED.{col}").format(col=sql.Identifier(col))
                    for col in columns if col != 'id'
                ])

                # Build SELECT clause with appropriate CASTs from TEXT staging table
                select_expressions = []
                target_types = {**COLUMN_TYPES, **self._custom_columns_dict}
                for col in columns:
                    full_type_definition = target_types.get(col, "TEXT")

                    base_pg_type = full_type_definition.split()[0].split('(')[0]
                    select_expressions.append(
                        sql.SQL("CAST(NULLIF({col}, '') AS {type})").format(
                            col=sql.Identifier(col),
                            type=sql.SQL(base_pg_type) 
                        )
                    )
                select_clause = sql.SQL(', ').join(select_expressions)

                upsert_sql = sql.SQL("""
                    INSERT INTO {target_table} ({insert_fields})
                    SELECT {select_clause}
                    FROM {staging_table}
                    ON CONFLICT (id) DO UPDATE SET
                        {update_assignments}
                    -- Optional WHERE clause can still be added if needed
                    -- WHERE EXCLUDED.update_time > {target_table}.update_time
                """).format(
                    target_table=target_table_id,
                    insert_fields=insert_fields,
                    select_clause=select_clause,
                    staging_table=staging_table_id,
                    update_assignments=update_assignments
                )

                cur.execute(upsert_sql)
                upserted_count = cur.rowcount
                conn.commit() 

                duration = py_time.monotonic() - start_time
                db_operation_duration_hist.labels(operation='upsert').observe(duration)
                self.log.info(
                    "Upsert completed successfully.",
                    record_count=record_count,
                    affected_rows=upserted_count, 
                    duration_sec=duration
                )

        except Exception as e:
            if conn: conn.rollback()
            self.log.error("Upsert failed", error=str(e), record_count=record_count, exc_info=True)
            raise 
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
                cur.execute(query, (list(ids_to_check),))
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

    def save_data(self, data: List[Dict]) -> None:
        """Default save implementation, uses upsert."""
        self.log.debug("Calling save_data, delegating to save_data_upsert.")
        self.save_data_upsert(data)