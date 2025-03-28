import csv
from io import StringIO
from application.ports.data_repository_port import DataRepositoryPort
from application.utils.data_transform import sanitize_column_name

# Definição dos campos base
BASE_COLUMNS = [
    "id", "titulo", "creator_user", "user_info", "person_info",
    "stage_id", "stage_name", "pipeline_id", "pipeline_name",
    "status", "value", "currency", "add_time", "update_time",
    "raw_data"
]

class PipedriveRepository(DataRepositoryPort):
    def __init__(self, db_pool, custom_field_mapping: dict):
        """
        Aqui, db_pool é uma instância de um pool de conexões.
        """
        self.db_pool = db_pool
        self.custom_field_mapping = {
            k: sanitize_column_name(v)
            for k, v in custom_field_mapping.items()
            if sanitize_column_name(v) not in set(BASE_COLUMNS)
        }
        self.create_table_and_indexes()

    def create_table_and_indexes(self):
        conn = self.db_pool.get_connection()
        try:
            with conn.cursor() as cursor:
                base_columns_sql = """
                    id TEXT PRIMARY KEY,
                    titulo TEXT,
                    creator_user JSONB,
                    user_info JSONB,
                    person_info JSONB,
                    stage_id INTEGER,
                    stage_name TEXT,
                    pipeline_id INTEGER,
                    pipeline_name TEXT,
                    status TEXT,
                    value NUMERIC,
                    currency TEXT,
                    add_time TIMESTAMP,
                    update_time TIMESTAMP,
                    raw_data JSONB
                """
                custom_columns_sql = ",\n".join(
                    [f"{sanitize_column_name(v)} TEXT" for v in self.custom_field_mapping.values()]
                )
                full_table_sql = f"""
                    CREATE TABLE IF NOT EXISTS pipedrive_data (
                        {base_columns_sql},
                        {custom_columns_sql}
                    );
                """
                cursor.execute(full_table_sql)
                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_titulo_gin
                    ON pipedrive_data USING gin (to_tsvector('portuguese', split_part(titulo, ' - ', 1)));
                """)
                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_stage_name
                    ON pipedrive_data (stage_name);
                """)
                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_pipeline_name
                    ON pipedrive_data (pipeline_name);
                """)
                conn.commit()
        finally:
            self.db_pool.release_connection(conn)

    def save_data_incremental(self, data):
        """
        Realiza um upsert incremental: insere novos registros ou atualiza os já existentes
        com base na chave primária (id) e no campo update_time.
        """
        column_names = BASE_COLUMNS + list(self.custom_field_mapping.values())
        
        # Preparar os dados em CSV
        csv_buffer = StringIO()
        writer = csv.writer(csv_buffer, quoting=csv.QUOTE_MINIMAL)
        writer.writerow(column_names)
        
        for record in data:
            base_fields = [
                str(record.get("id")),
                record.get("title"),
                record.get("creator_user_id"),
                record.get("user_id"),
                record.get("person_id"),
                record.get("stage_id"),
                (next(iter(record.get("stage", {}).values())).get("name") if record.get("stage") else None),
                record.get("pipeline_id"),
                record.get("pipeline_name"),
                record.get("status"),
                record.get("value"),
                record.get("currency"),
                record.get("add_time"),
                record.get("update_time"),
                record.get("raw_data")
            ]
            custom_values = []
            custom_data = record.get("custom_fields", {})
            for field_key in self.custom_field_mapping:
                value = custom_data.get(field_key)
                if isinstance(value, dict):
                    value = value.get("value", value)
                custom_values.append(value)
            writer.writerow(base_fields + custom_values)
        
        csv_buffer.seek(0)
        
        # Obter uma conexão e executar as operações em uma transação
        conn = self.db_pool.get_connection()
        try:
            with conn.cursor() as cursor:
                # 1. Criar tabela temporária de staging
                cursor.execute(f"""
                    CREATE TEMP TABLE staging_pipedrive_data (
                        {self._staging_table_definition()}
                    ) ON COMMIT DROP;
                """)
                
                # 2. Carregar dados na tabela de staging via COPY
                columns_csv = ", ".join(column_names)
                copy_sql = f"""
                    COPY staging_pipedrive_data({columns_csv})
                    FROM STDIN WITH CSV HEADER DELIMITER AS ','
                """
                cursor.copy_expert(copy_sql, csv_buffer)
                
                # 3. Upsert na tabela principal
                set_statements = []
                for col in column_names:
                    if col != "id":
                        set_statements.append(f"{col} = EXCLUDED.{col}")
                set_clause = ", ".join(set_statements)
                
                insert_sql = f"""
                    INSERT INTO pipedrive_data ({columns_csv})
                    SELECT {columns_csv} FROM staging_pipedrive_data
                    ON CONFLICT (id)
                    DO UPDATE SET
                        {set_clause}
                    WHERE pipedrive_data.update_time < EXCLUDED.update_time;
                """
                cursor.execute(insert_sql)
                conn.commit()
        finally:
            self.db_pool.release_connection(conn)

    def _staging_table_definition(self):
        """
        Retorna a definição completa das colunas para a tabela de staging.
        """
        base_columns_sql = """
            id TEXT PRIMARY KEY,
            titulo TEXT,
            creator_user JSONB,
            user_info JSONB,
            person_info JSONB,
            stage_id INTEGER,
            stage_name TEXT,
            pipeline_id INTEGER,
            pipeline_name TEXT,
            status TEXT,
            value NUMERIC,
            currency TEXT,
            add_time TIMESTAMP,
            update_time TIMESTAMP,
            raw_data JSONB
        """
        custom_columns_sql = ",\n".join(
            [f"{sanitize_column_name(v)} TEXT" for v in self.custom_field_mapping.values()]
        )
        if custom_columns_sql.strip():
            return f"{base_columns_sql},\n{custom_columns_sql}"
        else:
            return base_columns_sql

    # Para satisfazer a interface
    def save_data(self, data):
        return self.save_data_incremental(data)
