import json
import csv
import random
import time as py_time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set, Tuple
from io import StringIO

from psycopg2 import sql, extras
from psycopg2.extensions import cursor as DbCursor
import structlog

from application.ports.data_repository_port import DataRepositoryPort
from application.utils.column_utils import normalize_column_name
from infrastructure.db_pool import DBConnectionPool

log = structlog.get_logger(__name__)

# Constantes do domínio
BASE_COLUMNS = [ ... ]  # mesmo conteúdo que você já tem
COLUMN_TYPES = { ... }
LOOKUP_TABLE_USERS = "pipedrive_users"
LOOKUP_TABLE_PERSONS = "pipedrive_persons"
LOOKUP_TABLE_STAGES = "pipedrive_stages"
LOOKUP_TABLE_PIPELINES = "pipedrive_pipelines"
LOOKUP_TABLE_ORGANIZATIONS = "pipedrive_organizations"
UNKNOWN_NAME = "Desconhecido"

class PipedriveRepository(DataRepositoryPort):
    TABLE_NAME = "pipedrive_data"
    CONFIG_TABLE_NAME = "config"
    STAGING_TABLE_PREFIX = "staging_pipedrive_"
    STAGE_HISTORY_COLUMN_PREFIX = "moved_to_stage_"
    SCHEMA_LOCK_ID = 47835

    def __init__(
        self,
        db_pool: DBConnectionPool,
        custom_field_api_mapping: Dict[str, str],
        all_stages_details: List[Dict]
    ):
        self.db_pool = db_pool
        self.log = log.bind(repository="PipedriveRepository")
        self._raw_custom_field_mapping = custom_field_api_mapping
        self._all_stages_details = all_stages_details

        # Preparar colunas dinâmicas
        self._custom_columns_dict = self._prepare_custom_columns(custom_field_api_mapping)
        self._stage_history_columns_dict = self._prepare_stage_history_columns(
            all_stages_details,
            existing_custom_cols=set(self._custom_columns_dict.keys())
        )

        self.ensure_schema_exists()

    @property
    def custom_field_mapping(self) -> Dict[str, str]:
        """Retorna o mapeamento de custom fields."""
        return self._raw_custom_field_mapping

    # --- Métodos Auxiliares de Preparação de Colunas ---

    def _prepare_custom_columns(self, api_mapping: Dict[str, str]) -> Dict[str, str]:
        """Prepara os nomes e tipos de colunas customizadas, ignorando duplicatas ou nomes inválidos."""
        custom_cols = {}
        base_col_set = set(BASE_COLUMNS)
        reserved_prefixes = (self.STAGE_HISTORY_COLUMN_PREFIX,)

        for api_key, normalized_name in api_mapping.items():
            if not normalized_name or normalized_name == "_invalid_normalized_name":
                self.log.warning("Campo customizado com nome normalizado inválido; ignorando.",
                                 api_key=api_key, normalized_name=normalized_name)
                continue

            if normalized_name in base_col_set:
                self.log.warning("Nome normalizado do custom field conflita com coluna base; ignorando.",
                                 api_key=api_key, normalized_name=normalized_name)
                continue

            if any(normalized_name.startswith(prefix) for prefix in reserved_prefixes):
                self.log.warning("Nome normalizado do custom field usa prefixo reservado; ignorando.",
                                 api_key=api_key, normalized_name=normalized_name)
                continue

            custom_cols[normalized_name] = "TEXT"
        self.log.info("Custom columns preparadas", count=len(custom_cols))
        return custom_cols

    def _prepare_stage_history_columns(self, all_stages: List[Dict], existing_custom_cols: Set[str]) -> Dict[str, str]:
        """
        Prepara as colunas de histórico de stage, garantindo que nomes duplicados
        sejam diferenciados com sufixos (_2, _3, etc.).
        """
        stage_history_cols = {}
        if not all_stages:
            self.log.warning("Nenhum detalhe de stage fornecido; não é possível criar colunas de histórico.")
            return {}

        base_col_set = set(BASE_COLUMNS)
        forbidden_column_names = base_col_set.union(existing_custom_cols)
        normalized_name_counts: Dict[str, int] = {}

        for stage in all_stages:
            stage_id = stage.get('id')
            stage_name = stage.get('name')
            if not stage_id or not stage_name:
                self.log.warning("Entrada de stage sem id ou nome", stage_data=stage)
                continue

            try:
                normalized_stage_name = normalize_column_name(stage_name)
                if not normalized_stage_name or normalized_stage_name == "_invalid_normalized_name":
                    self.log.warning("Falha ao normalizar stage", stage_id=stage_id, stage_name=stage_name)
                    continue

                base_column_name = f"{self.STAGE_HISTORY_COLUMN_PREFIX}{normalized_stage_name}"
                final_column_name = base_column_name

                count = normalized_name_counts.get(normalized_stage_name, 0)
                if count > 0:
                    suffix = count + 1
                    final_column_name = f"{base_column_name}_{suffix}"
                    self.log.warning(
                        "Nome de stage duplicado detectado; usando sufixo.",
                        stage_id=stage_id, stage_name=stage_name,
                        normalized_name=normalized_stage_name,
                        original_column=base_column_name,
                        final_column=final_column_name
                    )

                normalized_name_counts[normalized_stage_name] = count + 1

                if final_column_name in forbidden_column_names:
                    self.log.error(
                        "CRÍTICO: Nome final de coluna de histórico conflita com coluna base/custom; ignorando.",
                        stage_id=stage_id, stage_name=stage_name,
                        normalized_name=normalized_stage_name,
                        final_column=final_column_name
                    )
                    normalized_name_counts[normalized_stage_name] -= 1
                    continue

                stage_history_cols[final_column_name] = "TIMESTAMPTZ"

            except Exception as e:
                self.log.error("Erro ao processar stage para coluna de histórico", stage_data=stage, error=str(e))

        self.log.info("Colunas de histórico preparadas", count=len(stage_history_cols))
        return stage_history_cols

    def _get_all_columns(self) -> List[str]:
        """Retorna a lista completa de colunas (base + custom + histórico)."""
        return BASE_COLUMNS + sorted(list(self._custom_columns_dict.keys())) + sorted(list(self._stage_history_columns_dict.keys()))

    # --- Métodos de Definição de Schema ---
    # Agora cada método de definição retorna uma tupla: (lista de SQL, dicionário esperado)

    def _get_main_table_column_definitions(self) -> Tuple[List[sql.SQL], Dict[str, str]]:
        """Gera definições SQL para a tabela principal."""
        defs = []
        expected = {}
        all_column_types = {**COLUMN_TYPES, **self._custom_columns_dict, **self._stage_history_columns_dict}
        ordered_cols = self._get_all_columns()

        for col in ordered_cols:
            col_type = all_column_types.get(col, "TEXT")
            defs.append(sql.SQL("{} {}").format(sql.Identifier(col), sql.SQL(col_type)))
            expected[col] = col_type

        return defs, expected

    def _get_config_column_definitions(self) -> Tuple[List[sql.SQL], Dict[str, str]]:
        """Gera definições para a tabela de configuração."""
        defs = [
            sql.SQL("key TEXT PRIMARY KEY"),
            sql.SQL("value JSONB"),
            sql.SQL("updated_at TIMESTAMPTZ DEFAULT NOW()")
        ]
        expected = {
            "key": "TEXT PRIMARY KEY",
            "value": "JSONB",
            "updated_at": "TIMESTAMPTZ DEFAULT NOW()"
        }
        return defs, expected

    def _get_lookup_table_definitions(self, table_name: str) -> Tuple[List[sql.SQL], Dict[str, str]]:
        """Gera definições SQL para tabelas de lookup."""
        if table_name == LOOKUP_TABLE_USERS:
            defs = [
                sql.SQL("user_id INTEGER PRIMARY KEY"),
                sql.SQL("user_name TEXT"),
                sql.SQL("is_active BOOLEAN"),
                sql.SQL("last_synced_at TIMESTAMPTZ DEFAULT NOW()")
            ]
            expected = {
                "user_id": "INTEGER PRIMARY KEY",
                "user_name": "TEXT",
                "is_active": "BOOLEAN",
                "last_synced_at": "TIMESTAMPTZ DEFAULT NOW()"
            }
            return defs, expected
        elif table_name == LOOKUP_TABLE_PERSONS:
            defs = [
                sql.SQL("person_id INTEGER PRIMARY KEY"),
                sql.SQL("person_name TEXT"),
                sql.SQL("org_id INTEGER"),
                sql.SQL("last_synced_at TIMESTAMPTZ DEFAULT NOW()")
            ]
            expected = {
                "person_id": "INTEGER PRIMARY KEY",
                "person_name": "TEXT",
                "org_id": "INTEGER",
                "last_synced_at": "TIMESTAMPTZ DEFAULT NOW()"
            }
            return defs, expected
        elif table_name == LOOKUP_TABLE_STAGES:
            defs = [
                sql.SQL("stage_id INTEGER PRIMARY KEY"),
                sql.SQL("stage_name TEXT"),
                sql.SQL("normalized_name TEXT"),
                sql.SQL("pipeline_id INTEGER"), 
                sql.SQL("order_nr INTEGER"),
                sql.SQL("is_active BOOLEAN"),
                sql.SQL("last_synced_at TIMESTAMPTZ DEFAULT NOW()")
            ]
            expected = {
                "stage_id": "INTEGER PRIMARY KEY",
                "stage_name": "TEXT",
                "normalized_name": "TEXT",
                "pipeline_id": "INTEGER",
                "order_nr": "INTEGER",
                "is_active": "BOOLEAN",
                "last_synced_at": "TIMESTAMPTZ DEFAULT NOW()"
            }
            return defs, expected
        elif table_name == LOOKUP_TABLE_PIPELINES:
            defs = [
                sql.SQL("pipeline_id INTEGER PRIMARY KEY"),
                sql.SQL("pipeline_name TEXT"),
                sql.SQL("is_active BOOLEAN"),
                sql.SQL("last_synced_at TIMESTAMPTZ DEFAULT NOW()")
            ]
            expected = {
                "pipeline_id": "INTEGER PRIMARY KEY",
                "pipeline_name": "TEXT",
                "is_active": "BOOLEAN",
                "last_synced_at": "TIMESTAMPTZ DEFAULT NOW()"
            }
            return defs, expected
        elif table_name == LOOKUP_TABLE_ORGANIZATIONS:
            defs = [
                sql.SQL("org_id INTEGER PRIMARY KEY"),
                sql.SQL("org_name TEXT"),
                sql.SQL("last_synced_at TIMESTAMPTZ DEFAULT NOW()")
            ]
            expected = {
                "org_id": "INTEGER PRIMARY KEY",
                "org_name": "TEXT",
                "last_synced_at": "TIMESTAMPTZ DEFAULT NOW()"
            }
            return defs, expected
        else:
            raise ValueError(f"Unknown lookup table name: {table_name}")

    def ensure_schema_exists(self):
        """Garante que as tabelas (principal, config e lookups) e índices existam."""
        conn = None
        locked = False
        try:
            conn = self.db_pool.get_connection()
            with conn.cursor() as cur:
                # 1. Adquire lock para modificação do schema
                cur.execute("SELECT pg_advisory_lock(%s)", (self.SCHEMA_LOCK_ID,))
                locked = True
                self.log.info("Acquired schema modification lock.", lock_id=self.SCHEMA_LOCK_ID)

                # 2. Criar/Alterar tabela principal
                self._create_or_alter_table(cur, self.TABLE_NAME, self._get_main_table_column_definitions)

                # 3. Criar/Alterar tabela de configuração
                self._create_or_alter_table(cur, self.CONFIG_TABLE_NAME, self._get_config_column_definitions)

                # 4. Criar/Alterar tabelas de lookup
                lookup_tables = [
                    LOOKUP_TABLE_USERS, LOOKUP_TABLE_PERSONS, LOOKUP_TABLE_STAGES,
                    LOOKUP_TABLE_PIPELINES, LOOKUP_TABLE_ORGANIZATIONS
                ]
                for table in lookup_tables:
                    self._create_or_alter_table(cur, table, lambda t=table: self._get_lookup_table_definitions(t))

                # 5. Criar índices
                self._create_indexes(cur)

                conn.commit()
                self.log.info("Schema check/modification committed for all tables.")

        except Exception as e:
            if conn:
                conn.rollback()
            self.log.critical("Failed to ensure database schema", error=str(e), exc_info=True)
            raise
        finally:
            if conn:
                if locked:
                    try:
                        with conn.cursor() as unlock_cur:
                            unlock_cur.execute("SELECT pg_advisory_unlock(%s)", (self.SCHEMA_LOCK_ID,))
                        conn.commit()
                        self.log.info("Released schema modification lock.", lock_id=self.SCHEMA_LOCK_ID)
                    except Exception as unlock_err:
                        self.log.error("Failed to release schema lock", error=str(unlock_err))
                self.db_pool.release_connection(conn)

    def _create_or_alter_table(self, cur: DbCursor, table_name: str, get_col_defs_func: callable):
        """
        Função genérica para criar uma tabela ou adicionar colunas faltantes.
        Utiliza o dicionário 'expected_columns_map' retornado pela função de definição de colunas.
        """
        table_id = sql.Identifier(table_name)
        log_ctx = self.log.bind(table_name=table_name)
        log_ctx.debug("Starting schema check/update for table.")

        # Verifica se a tabela existe
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_schema = 'public' AND table_name = %s
            );
        """, (table_name,))
        table_exists = cur.fetchone()[0]

        # Obtém as definições de colunas e o mapeamento esperado
        col_defs_result = get_col_defs_func()
        if isinstance(col_defs_result, tuple) and len(col_defs_result) == 2:
            column_defs_sql, expected_columns_map = col_defs_result
        else:
            column_defs_sql = col_defs_result
            expected_columns_map = {}
            for col_def in column_defs_sql:
                col_def_str = col_def.as_string(cur)
                tokens = col_def_str.split()
                if tokens:
                    col_name = tokens[0].strip('"')
                    expected_columns_map[col_name] = " ".join(tokens[1:])

        if not table_exists:
            log_ctx.info("Table does not exist, creating.")
            if not column_defs_sql:
                raise RuntimeError(f"Cannot create table '{table_name}' with no column definitions.")
            create_sql = sql.SQL("CREATE TABLE {table} ({columns})").format(
                table=table_id,
                columns=sql.SQL(',\n    ').join(column_defs_sql)
            )
            log_ctx.debug("Executing CREATE TABLE", sql_query=create_sql.as_string(cur))
            cur.execute(create_sql)
            log_ctx.info("Table created successfully.")
        else:
            log_ctx.debug("Table exists, checking for missing columns.")
            cur.execute("""
                SELECT column_name FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = %s;
            """, (table_name,))
            existing_columns = {row[0] for row in cur.fetchall()}
            missing_columns = set(expected_columns_map.keys()) - existing_columns

            if missing_columns:
                log_ctx.info("Adding missing columns to table.", missing=sorted(list(missing_columns)))
                alter_statements = []
                for col in sorted(missing_columns):
                    col_type = expected_columns_map[col]
                    alter_statements.append(
                        sql.SQL("ADD COLUMN IF NOT EXISTS {} {}").format(
                            sql.Identifier(col),
                            sql.SQL(col_type)
                        )
                    )
                if alter_statements:
                    alter_sql = sql.SQL("ALTER TABLE {table} ").format(table=table_id) + sql.SQL(', ').join(alter_statements)
                    try:
                        log_ctx.debug("Executing ALTER TABLE ADD COLUMN(s)", columns=sorted(list(missing_columns)))
                        cur.execute(alter_sql)
                    except Exception as alter_err:
                        log_ctx.warning("Bulk ALTER TABLE failed, attempting one by one.", error=str(alter_err))
                        conn = cur.connection
                        if conn:
                            conn.rollback()
                        for stmt in alter_statements:
                            single_alter_sql = sql.SQL("ALTER TABLE {table} ").format(table=table_id) + stmt
                            try:
                                log_ctx.debug("Executing ALTER TABLE ADD COLUMN (single)", statement=stmt.as_string(cur))
                                cur.execute(single_alter_sql)
                            except Exception as single_alter_err:
                                log_ctx.error("Failed to add column individually", column=col, error=str(single_alter_err))
                        if conn:
                            conn.commit()
                log_ctx.info("Missing columns check/addition process completed.", count=len(missing_columns))
            else:
                log_ctx.debug("No missing columns found.")

    def _create_indexes(self, cur: DbCursor):
        """Cria os índices padrão para a tabela principal e as tabelas de lookup."""
        self.log.debug("Starting index creation process.")

        main_table_indexes = {
            f"idx_{self.TABLE_NAME}_update_time": sql.SQL("(update_time DESC)"),
            f"idx_{self.TABLE_NAME}_stage_id": sql.SQL("(stage_id)"),
            f"idx_{self.TABLE_NAME}_pipeline_id": sql.SQL("(pipeline_id)"),
            f"idx_{self.TABLE_NAME}_person_id": sql.SQL("(person_id)"),
            f"idx_{self.TABLE_NAME}_org_id": sql.SQL("(org_id)"),
            f"idx_{self.TABLE_NAME}_owner_id": sql.SQL("(owner_id)"),
            f"idx_{self.TABLE_NAME}_creator_user_id": sql.SQL("(creator_user_id)"),
            f"idx_{self.TABLE_NAME}_status": sql.SQL("(status)"),
            f"idx_{self.TABLE_NAME}_add_time": sql.SQL("(add_time DESC)"),
            f"idx_{self.TABLE_NAME}_active_deals_update": sql.SQL("(update_time DESC) WHERE status NOT IN ('Ganho', 'Perdido', 'Deletado')")
        }
        self._apply_indexes(cur, self.TABLE_NAME, main_table_indexes)

        lookup_tables_indexes = {
            LOOKUP_TABLE_USERS: { f"idx_{LOOKUP_TABLE_USERS}_name": sql.SQL("(user_name text_pattern_ops)") },
            LOOKUP_TABLE_PERSONS: { f"idx_{LOOKUP_TABLE_PERSONS}_name": sql.SQL("(person_name text_pattern_ops)") },
            LOOKUP_TABLE_STAGES: { f"idx_{LOOKUP_TABLE_STAGES}_norm_name": sql.SQL("(normalized_name)") },
            LOOKUP_TABLE_PIPELINES: {},
            LOOKUP_TABLE_ORGANIZATIONS: { f"idx_{LOOKUP_TABLE_ORGANIZATIONS}_name": sql.SQL("(org_name text_pattern_ops)") },
        }
        for table_name, indexes in lookup_tables_indexes.items():
            self._apply_indexes(cur, table_name, indexes)

        self.log.debug("Index creation process completed.")

    def _apply_indexes(self, cur: DbCursor, table_name: str, indexes_to_create: Dict[str, sql.SQL]):
        """Helper para criação de índices em uma tabela específica."""
        table_id = sql.Identifier(table_name)
        log_ctx = self.log.bind(table_name=table_name)
        for idx_name, idx_definition in indexes_to_create.items():
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
                    WHERE c.relname = %s AND c.relkind = 'i' AND n.nspname = 'public'
                );
            """, (idx_name,))
            index_exists = cur.fetchone()[0]

            if not index_exists:
                create_idx_sql = sql.SQL("CREATE INDEX IF NOT EXISTS {} ON {} {}").format(
                    sql.Identifier(idx_name),
                    table_id,
                    idx_definition
                )
                try:
                    log_ctx.debug("Creating index.", index_name=idx_name)
                    cur.execute(create_idx_sql)
                except Exception as idx_err:
                    log_ctx.warning("Failed to create index", index_name=idx_name, error=str(idx_err))
            else:
                log_ctx.debug("Index already exists.", index_name=idx_name)

    # --- Métodos de Upsert para Tabelas de Lookup ---
    def _upsert_lookup_data(self, table_name: str, data: List[Dict], id_column: str, columns: List[str]):
        """Função genérica para fazer upsert em tabelas de lookup."""
        if not data:
            self.log.debug("No data provided for lookup upsert", table_name=table_name)
            return 0

        conn = None
        start_time = py_time.monotonic()
        rows_affected = 0
        table_id = sql.Identifier(table_name)
        col_ids = sql.SQL(', ').join(map(sql.Identifier, columns))
        update_cols = sql.SQL(', ').join(
            sql.SQL("{col} = EXCLUDED.{col}").format(col=sql.Identifier(col))
            for col in columns if col != id_column # Não atualiza o ID
        )
        # Adicionar atualização de last_synced_at
        update_cols += sql.SQL(", last_synced_at = NOW()")

        upsert_sql_template = sql.SQL("""
            INSERT INTO {table} ({insert_cols})
            VALUES %s
            ON CONFLICT ({pk_col}) DO UPDATE SET {update_assignments}
        """).format(
            table=table_id,
            insert_cols=col_ids,
            pk_col=sql.Identifier(id_column),
            update_assignments=update_cols
        )

        # Preparar dados como tuplas na ordem das colunas
        values_tuples = []
        for record in data:
            row = []
            for col in columns:
                val = record.get(col)
                # Tratamento básico de tipos para SQL
                if isinstance(val, datetime):
                    val = val.isoformat()
                elif val is None:
                    val = None # psycopg2 lida com None -> NULL
                # TODO: Adicionar mais tratamentos se necessário (booleanos, etc.)
                row.append(val)
            values_tuples.append(tuple(row))

        try:
            conn = self.db_pool.get_connection()
            with conn.cursor() as cur:
                # Usar execute_values para eficiência
                extras.execute_values(cur, upsert_sql_template.as_string(cur), values_tuples, page_size=500)
                rows_affected = cur.rowcount
                conn.commit()
                duration = py_time.monotonic() - start_time
                self.log.info("Upsert successful for lookup table", table_name=table_name,
                              records_processed=len(data), rows_affected=rows_affected, duration_sec=f"{duration:.3f}s")
                return rows_affected
        except Exception as e:
            if conn: conn.rollback()
            self.log.error("Upsert failed for lookup table", table_name=table_name, error=str(e), record_count=len(data), exc_info=True)
            raise
        finally:
            if conn:
                self.db_pool.release_connection(conn)

    # Métodos específicos por tabela de lookup
    def upsert_users(self, data: List[Dict]):
        """Faz upsert na tabela pipedrive_users."""
        cols = ['user_id', 'user_name', 'is_active'] # Colunas esperadas no dict `data`
        # Mapear 'id' da API para 'user_id', 'name' para 'user_name', 'active_flag' para 'is_active'
        mapped_data = [
            {'user_id': r['id'], 'user_name': r.get('name', self.UNKNOWN_NAME), 'is_active': r.get('active_flag', True)}
            for r in data if 'id' in r
        ]
        return self._upsert_lookup_data(LOOKUP_TABLE_USERS, mapped_data, 'user_id', cols)

    def upsert_persons(self, data: List[Dict]):
        """Faz upsert na tabela pipedrive_persons."""
        cols = ['person_id', 'person_name', 'org_id'] # Ajustar se sincronizar mais campos
        mapped_data = [
            {
                'person_id': r['id'],
                'person_name': r.get('name', self.UNKNOWN_NAME),
                # Pega o ID da org se for um dict, senão None
                'org_id': r.get('org_id', {}).get('value') if isinstance(r.get('org_id'), dict) else r.get('org_id')
            }
            for r in data if 'id' in r
        ]
        return self._upsert_lookup_data(LOOKUP_TABLE_PERSONS, mapped_data, 'person_id', cols)

    def upsert_stages(self, data: List[Dict]):
        """Faz upsert na tabela pipedrive_stages."""
        cols = ['stage_id', 'stage_name', 'normalized_name', 'pipeline_id', 'order_nr', 'is_active']
        mapped_data = []
        for r in data:
            if 'id' in r:
                 stage_name = r.get('name', self.UNKNOWN_NAME)
                 normalized = normalize_column_name(stage_name) if stage_name != self.UNKNOWN_NAME else None
                 mapped_data.append({
                     'stage_id': r['id'],
                     'stage_name': stage_name,
                     'normalized_name': normalized,
                     'pipeline_id': r.get('pipeline_id'),
                     'order_nr': r.get('order_nr'),
                     'is_active': r.get('active_flag', True) # Verificar nome correto do campo na API
                 })
        return self._upsert_lookup_data(LOOKUP_TABLE_STAGES, mapped_data, 'stage_id', cols)

    def upsert_pipelines(self, data: List[Dict]):
        """Faz upsert na tabela pipedrive_pipelines."""
        cols = ['pipeline_id', 'pipeline_name', 'is_active']
        mapped_data = [
            {
                'pipeline_id': r['id'],
                'pipeline_name': r.get('name', self.UNKNOWN_NAME),
                'is_active': r.get('active_flag', True) # Verificar nome correto do campo na API
            }
             for r in data if 'id' in r
        ]
        return self._upsert_lookup_data(LOOKUP_TABLE_PIPELINES, mapped_data, 'pipeline_id', cols)

    def upsert_organizations(self, data: List[Dict]):
        """Faz upsert na tabela pipedrive_organizations."""
        cols = ['org_id', 'org_name'] # Ajustar se sincronizar mais campos
        mapped_data = [
            {'org_id': r['id'], 'org_name': r.get('name', self.UNKNOWN_NAME)}
            for r in data if 'id' in r
        ]
        return self._upsert_lookup_data(LOOKUP_TABLE_ORGANIZATIONS, mapped_data, 'org_id', cols)
    
    # --- Método de Consulta para o ETL Principal ---
    def get_lookup_maps_for_batch(
        self,
        user_ids: Optional[Set[int]] = None,
        person_ids: Optional[Set[int]] = None,
        stage_ids: Optional[Set[int]] = None,
        pipeline_ids: Optional[Set[int]] = None,
        org_ids: Optional[Set[int]] = None
        ) -> Dict[str, Dict[int, str]]:
        """
        Busca nomes das tabelas de lookup persistentes para os IDs fornecidos.
        Retorna um dicionário onde as chaves são 'users', 'persons', etc.,
        e os valores são dicionários {id: name}.
        """
        results = {
            'users': {}, 'persons': {}, 'stages': {}, 'pipelines': {}, 'orgs': {}
        }
        if not any([user_ids, person_ids, stage_ids, pipeline_ids, org_ids]):
            return results 

        conn = None
        try:
            conn = self.db_pool.get_connection()
            with conn.cursor() as cur:
                if user_ids:
                    cur.execute(sql.SQL("SELECT user_id, user_name FROM {} WHERE user_id = ANY(%s)").format(sql.Identifier(LOOKUP_TABLE_USERS)), (list(user_ids),))
                    results['users'] = {row[0]: row[1] for row in cur.fetchall()}
                if person_ids:
                    cur.execute(sql.SQL("SELECT person_id, person_name FROM {} WHERE person_id = ANY(%s)").format(sql.Identifier(LOOKUP_TABLE_PERSONS)), (list(person_ids),))
                    results['persons'] = {row[0]: row[1] for row in cur.fetchall()}
                if stage_ids:
                    cur.execute(sql.SQL("SELECT stage_id, normalized_name FROM {} WHERE stage_id = ANY(%s)").format(sql.Identifier(LOOKUP_TABLE_STAGES)), (list(stage_ids),))
                    results['stages'] = {row[0]: row[1] for row in cur.fetchall() if row[1]}
                if pipeline_ids:
                    cur.execute(sql.SQL("SELECT pipeline_id, pipeline_name FROM {} WHERE pipeline_id = ANY(%s)").format(sql.Identifier(LOOKUP_TABLE_PIPELINES)), (list(pipeline_ids),))
                    results['pipelines'] = {row[0]: row[1] for row in cur.fetchall()}
                if org_ids:
                     cur.execute(sql.SQL("SELECT org_id, org_name FROM {} WHERE org_id = ANY(%s)").format(sql.Identifier(LOOKUP_TABLE_ORGANIZATIONS)), (list(org_ids),))
                     results['orgs'] = {row[0]: row[1] for row in cur.fetchall()}

            self.log.debug("Fetched lookup maps for batch from DB",
                           user_count=len(results['users']), person_count=len(results['persons']),
                           stage_count=len(results['stages']), pipeline_count=len(results['pipelines']),
                           org_count=len(results['orgs']))
            return results

        except Exception as e:
            self.log.error("Failed to get lookup maps for batch", error=str(e), exc_info=True)
            return {'users': {}, 'persons': {}, 'stages': {}, 'pipelines': {}, 'orgs': {}}
        finally:
            if conn:
                self.db_pool.release_connection(conn)


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

    # --- Métodos da Tabela Principal (pipedrive_data) ---

    def save_data_upsert(self, data: List[Dict]):
        """Upsert eficiente para a tabela principal `pipedrive_data` (código original mantido)."""
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
        self.log.debug("Starting upsert process for main table", record_count=record_count, column_count=len(columns), target_table=self.TABLE_NAME)

        try:
            conn = self.db_pool.get_connection()
            with conn.cursor() as cur:
                # 1. Criar tabela de staging UNLOGGED (mais rápida)
                staging_col_defs = [sql.SQL("{} TEXT").format(sql.Identifier(col)) for col in columns]
                create_staging_sql = sql.SQL("""
                    CREATE UNLOGGED TABLE {staging_table} ( {columns} )
                """).format(
                    staging_table=staging_table_id,
                    columns=sql.SQL(',\n    ').join(staging_col_defs)
                )
                self.log.debug("Creating unlogged staging table.", table_name=staging_table_name)
                cur.execute(create_staging_sql)

                # 2. Preparar dados e usar COPY
                buffer = StringIO()
                copy_failed_records = 0
                try:
                    writer = csv.writer(buffer, delimiter='|', quoting=csv.QUOTE_MINIMAL, lineterminator='\n')
                    for i, record in enumerate(data):
                        row = []
                        try:
                            for field in columns:
                                value = record.get(field)
                                if value is None:
                                    row.append('\\N') 
                                elif isinstance(value, datetime):
                                    if value.tzinfo is None:
                                        value = value.replace(tzinfo=timezone.utc)
                                    else:
                                        value = value.astimezone(timezone.utc)
                                    row.append(value.isoformat(timespec='microseconds'))
                                elif isinstance(value, bool):
                                    row.append('t' if value else 'f')
                                elif isinstance(value, (dict, list)):
                                     row.append(json.dumps(value).replace('\\', '\\\\').replace('|', '\\|').replace('\n', '\\n').replace('\r', '\\r'))
                                else:
                                    str_value = str(value)
                                    escaped_value = str_value.replace('\\', '\\\\').replace('|', '\\|').replace('\n', '\\n').replace('\r', '\\r')
                                    row.append(escaped_value)
                            writer.writerow(row)
                        except Exception as row_err:
                             copy_failed_records += 1
                             self.log.error("Error preparing record for COPY", record_index=i, error=str(row_err), record_preview=str(record)[:200])
                    buffer.seek(0)

                    if copy_failed_records > 0:
                        self.log.warning("Some records failed preparation for COPY", failed_count=copy_failed_records, total_records=record_count)

                    # 3. Executar COPY
                    copy_sql = sql.SQL("COPY {staging_table} ({fields}) FROM STDIN WITH (FORMAT CSV, DELIMITER '|', NULL '\\N', ENCODING 'UTF8')").format(
                        staging_table=staging_table_id,
                        fields=sql.SQL(', ').join(map(sql.Identifier, columns))
                    )
                    self.log.debug("Executing COPY command.", table_name=staging_table_name)
                    cur.copy_expert(copy_sql, buffer)
                    copy_row_count = cur.rowcount
                    self.log.debug("Copied data to staging table.", copied_row_count=copy_row_count, expected_count=record_count - copy_failed_records, table_name=staging_table_name)

                finally:
                    buffer.close()

                # 4. Executar UPSERT com CASTING
                insert_fields = sql.SQL(', ').join(map(sql.Identifier, columns))
                update_assignments_list = []
                for col in columns:
                    if col == 'id': continue
                    if col == 'add_time':
                         update_assignments_list.append(
                             sql.SQL("{col} = COALESCE({target}.{col}, EXCLUDED.{col})").format(
                                 col=sql.Identifier(col), target=target_table_id
                             )
                         )
                         continue
                    update_assignments_list.append(sql.SQL("{col} = EXCLUDED.{col}").format(col=sql.Identifier(col)))

                update_assignments = sql.SQL(', ').join(update_assignments_list)

                # Mapear tipos da tabela principal para casting
                target_types_main = {**COLUMN_TYPES, **self._custom_columns_dict, **self._stage_history_columns_dict}
                select_expressions = []
                for col in columns:
                    full_type_definition = target_types_main.get(col, "TEXT")
                    base_pg_type = full_type_definition.split()[0].split('(')[0].upper()

                    if base_pg_type in ('INTEGER', 'BIGINT', 'NUMERIC', 'DECIMAL', 'REAL', 'DOUBLE PRECISION', 'DATE', 'TIMESTAMP', 'TIMESTAMPTZ', 'BOOLEAN'):
                        select_expressions.append(
                            sql.SQL("NULLIF(TRIM({col}), '')::{type}").format(
                                col=sql.Identifier(col),
                                type=sql.SQL(base_pg_type) 
                            )
                        )
                    elif base_pg_type == 'JSONB': 
                         select_expressions.append(
                             sql.SQL("NULLIF(TRIM({col}), '')::JSONB").format(col=sql.Identifier(col))
                         )
                    else: 
                         select_expressions.append(sql.Identifier(col))

                select_clause = sql.SQL(', ').join(select_expressions)

                upsert_sql = sql.SQL("""
                    INSERT INTO {target_table} ({insert_fields})
                    SELECT {select_clause} FROM {staging_table}
                    ON CONFLICT (id) DO UPDATE SET {update_assignments}
                    WHERE {target_table}.update_time IS NULL OR EXCLUDED.update_time >= {target_table}.update_time
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

                duration = py_time.monotonic() - start_time
                self.log.info(
                    "Upsert completed successfully for main table.",
                    record_count=record_count,
                    initial_copy_failures=copy_failed_records,
                    affected_rows_upsert=upserted_count,
                    duration_sec=f"{duration:.3f}"
                )

        except Exception as e:
            if conn: conn.rollback()
            self.log.error("Upsert failed for main table", error=str(e), record_count=record_count, exc_info=True)
            raise 
        finally:
            # 5. Dropar tabela de staging sempre
            if conn:
                try:
                    with conn.cursor() as final_cur:
                         drop_sql = sql.SQL("DROP TABLE IF EXISTS {staging_table}").format(staging_table=staging_table_id)
                         self.log.debug("Dropping staging table.", table_name=staging_table_name)
                         final_cur.execute(drop_sql)
                         conn.commit() # Commit do drop
                except Exception as drop_err:
                     self.log.error("Failed to drop staging table", table_name=staging_table_name, error=str(drop_err))
                finally:
                    self.db_pool.release_connection(conn) # Liberar conexão

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
                duration = py_time.monotonic() - start_time
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