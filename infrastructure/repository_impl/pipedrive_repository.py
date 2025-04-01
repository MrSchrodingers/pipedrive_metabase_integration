import json
import csv
from typing import List, Dict
from io import StringIO
from psycopg2 import sql
from psycopg2.extensions import cursor
from prefect import get_run_logger
from application.ports.data_repository_port import DataRepositoryPort
from application.utils.column_utils import normalize_column_name
from infrastructure.monitoring.metrics import insert_duration

logger = get_run_logger()

BASE_COLUMNS = [
    "id", "titulo", "creator_user", "user_info", "person_info",
    "stage_id", "stage_name", "pipeline_id", "pipeline_name",
    "status", "value", "currency", "add_time", "update_time", "raw_data"
]

class PipedriveRepository(DataRepositoryPort):
    def __init__(self, db_pool, custom_field_mapping: dict):
        self.db_pool = db_pool
        self.custom_field_mapping = {
            k: self._sanitize_column_name(v)
            for k, v in custom_field_mapping.items()
            if self._sanitize_column_name(v) not in set(BASE_COLUMNS)
        }
        self._ensure_table_exists()

    def _sanitize_column_name(self, name: str) -> str:
        return normalize_column_name(name)

    def _ensure_table_exists(self):
        """Criação de tabela com lock advisory para concorrência"""
        conn = self.db_pool.get_connection()
        try:
            with conn.cursor() as cur, conn:
                cur.execute("SELECT pg_advisory_lock(12345)")
                self._create_table(cur)
                self._create_indexes(cur)
                cur.execute("SELECT pg_advisory_unlock(12345)")
        finally:
            self.db_pool.release_connection(conn)

    def _create_table(self, cur: cursor):
        """Criação da tabela principal com campos dinâmicos"""
        columns = [
            sql.SQL("{} TEXT PRIMARY KEY").format(sql.Identifier("id")),
            sql.SQL("titulo TEXT"),
            sql.SQL("creator_user JSONB"),
            sql.SQL("user_info JSONB"),
            sql.SQL("person_info JSONB"),
            sql.SQL("stage_id INTEGER"),
            sql.SQL("stage_name TEXT"),
            sql.SQL("pipeline_id INTEGER"),
            sql.SQL("pipeline_name TEXT"),
            sql.SQL("status TEXT"),
            sql.SQL("value NUMERIC"),
            sql.SQL("currency TEXT"),
            sql.SQL("add_time TIMESTAMP"),
            sql.SQL("update_time TIMESTAMP"),
            sql.SQL("raw_data JSONB")
        ]

        # Adicionar campos customizados
        for col_name in self.custom_field_mapping.values():
            columns.append(sql.SQL("{} TEXT").format(sql.Identifier(col_name)))

        create_table = sql.SQL("""
            CREATE TABLE IF NOT EXISTS pipedrive_data (
                {columns}
            )
        """).format(columns=sql.SQL(",\n").join(columns))

        cur.execute(create_table)
        logger.info("Tabela pipedrive_data criada/verificada")

    def _create_indexes(self, cur: cursor):
        """Criação de índices otimizados"""
        indexes = [
            ("idx_stage_name", "(stage_name)"),
            ("idx_pipeline_name", "(pipeline_name)"),
            ("idx_update_time", "(update_time)")
        ]

        for name, definition in indexes:
            cur.execute(sql.SQL(
                "CREATE INDEX IF NOT EXISTS {} ON pipedrive_data {}"
            ).format(sql.Identifier(name), sql.SQL(definition)))
        
        logger.info("Índices criados/verificados")

    def save_data_incremental(self, data: List[Dict]):
        """Inserção otimizada usando COPY binário"""
        if not data:
            return

        conn = self.db_pool.get_connection()
        try:
            with conn.cursor() as cur, StringIO() as buffer:
                # Gerar dados binários
                for record in data:
                    buffer.write(self._record_to_line(record) + '\n')
                buffer.seek(0)

                # Executar COPY
                columns = BASE_COLUMNS + list(self.custom_field_mapping.values())
                copy_sql = sql.SQL("""
                    COPY pipedrive_data ({fields})
                    FROM STDIN WITH (FORMAT CSV, DELIMITER '|', NULL '')
                """).format(fields=sql.SQL(', ').join(
                    map(sql.Identifier, columns)
                ))

                with insert_duration.time():
                    cur.copy_expert(copy_sql, buffer)
                    conn.commit()

                logger.info(f"Dados inseridos: {len(data)} registros")

        except Exception as e:
            logger.error(f"Falha na inserção: {str(e)}")
            conn.rollback()
            raise
        finally:
            self.db_pool.release_connection(conn)
            
    def filter_existing_records(self, data: List[Dict]) -> List[Dict]:
        conn = self.db_pool.get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT id FROM pipedrive_data WHERE id = ANY(%s)", ([rec["id"] for rec in data],))
                existing = {row[0] for row in cur.fetchall()}
            return [rec for rec in data if rec["id"] not in existing]
        finally:
            self.db_pool.release_connection(conn)
            
    def save_data_upsert(self, data: List[Dict]):
        """Insere dados usando um staging table e depois faz upsert na tabela principal."""
        if not data:
            return

        conn = self.db_pool.get_connection()
        staging_table = "staging_pipedrive_data"
        try:
            with conn.cursor() as cur:
                # Cria uma tabela temporária para staging
                cur.execute(f"DROP TABLE IF EXISTS {staging_table}")
                columns = BASE_COLUMNS + list(self.custom_field_mapping.values())
                create_staging = sql.SQL("""
                    CREATE TEMPORARY TABLE {staging} (
                        {columns}
                    ) ON COMMIT DROP
                """).format(
                    staging=sql.Identifier(staging_table),
                    columns=sql.SQL(",\n").join(
                        [sql.SQL("{} TEXT").format(sql.Identifier(col)) for col in columns]
                    )
                )
                cur.execute(create_staging)
                logger.info("Tabela de staging %s criada.", staging_table)

                # Prepara os dados usando a mesma CSV writer
                buffer = StringIO()
                for record in data:
                    buffer.write(self._record_to_line(record) + '\n')
                buffer.seek(0)

                # Copia os dados para a tabela de staging
                copy_sql = sql.SQL("""
                    COPY {staging} ({fields})
                    FROM STDIN WITH (FORMAT CSV, DELIMITER '|', NULL '')
                """).format(
                    staging=sql.Identifier(staging_table),
                    fields=sql.SQL(', ').join(map(sql.Identifier, columns))
                )
                cur.copy_expert(copy_sql, buffer)
                logger.info("Dados copiados para a tabela de staging.")

                target_column_types = {
                    "creator_user": "JSONB",
                    "user_info": "JSONB",
                    "person_info": "JSONB",
                    "stage_id": "INTEGER",
                    "pipeline_id": "INTEGER",
                    "value": "NUMERIC",
                    "add_time": "TIMESTAMP",
                    "update_time": "TIMESTAMP",
                    "raw_data": "JSONB"
                }
                select_fields = []
                for col in columns:
                    if col in target_column_types:
                        # Aplica o cast para o tipo correto
                        select_fields.append(
                            sql.SQL("CAST({col} AS {typ})").format(
                                col=sql.Identifier(col),
                                typ=sql.SQL(target_column_types[col])
                            )
                        )
                    else:
                        select_fields.append(sql.Identifier(col))
                select_fields_sql = sql.SQL(', ').join(select_fields)

                # Monta a lista de campos para INSERT e UPDATE
                insert_fields = sql.SQL(', ').join(map(sql.Identifier, columns))
                update_clause = sql.SQL(', ').join([
                    sql.SQL("{col} = EXCLUDED.{col}").format(col=sql.Identifier(col))
                    for col in columns if col != "id"
                ])
                upsert_sql = sql.SQL("""
                    INSERT INTO pipedrive_data ({fields})
                    SELECT {select_fields} FROM {staging}
                    ON CONFLICT (id) DO UPDATE SET
                    {update_clause}
                """).format(
                    fields=insert_fields,
                    select_fields=select_fields_sql,
                    staging=sql.Identifier(staging_table),
                    update_clause=update_clause
                )
                with insert_duration.time():
                    cur.execute(upsert_sql)
                    conn.commit()
                logger.info("Upsert realizado com sucesso para %d registros.", len(data))
        except Exception as e:
            logger.error(f"Falha na inserção via upsert: {str(e)}")
            conn.rollback()
            raise
        finally:
            self.db_pool.release_connection(conn)

    def _record_to_line(self, record: Dict) -> str:
        output = StringIO()
        writer = csv.writer(output, delimiter='|', quoting=csv.QUOTE_MINIMAL)
        row = []
        jsonb_fields = {'user_info', 'person_info', 'raw_data'}
        for field in BASE_COLUMNS + list(self.custom_field_mapping.values()):
            value = record.get(field)
            if value is None:
                row.append('')
            elif field in jsonb_fields:
                try:
                    json_str = json.dumps(value, ensure_ascii=False)
                except (TypeError, ValueError):
                    json_str = 'null'
                row.append(json_str)
            else:
                row.append(str(value))
        writer.writerow(row)
        return output.getvalue().strip("\r\n")

    def save_data(self, data):
        self.save_data_incremental(data)