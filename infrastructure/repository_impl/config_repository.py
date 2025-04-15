from typing import Dict, Any, Optional
from psycopg2 import sql, pool, extras
import structlog
import json
from datetime import datetime, timezone

from infrastructure.db.schema_manager import SchemaManager

class ConfigRepository:
    """Repository for managing key-value configurations stored in the database."""
    TABLE_NAME = "config"
    COLUMNS = {
        "key": "TEXT PRIMARY KEY",
        "value": "JSONB",
        "updated_at": "TIMESTAMPTZ DEFAULT NOW()"
    }

    def __init__(self, db_pool: pool.SimpleConnectionPool, schema_manager: SchemaManager):
        self.db_pool = db_pool
        self.schema_manager = schema_manager
        self.log = structlog.get_logger(__name__).bind(repository=self.__class__.__name__)

    def initialize_schema(self):
        """Initializes the schema for the config table."""
        self.schema_manager.ensure_table_exists(
            self.TABLE_NAME,
            list(self.COLUMNS.items()),
            primary_key="key"
        )
        self.log.info("Schema initialized", table_name=self.TABLE_NAME)

    def save_configuration(self, key: str, value: Dict[str, Any]):
        """Saves or updates a configuration value (JSONB) in the database."""
        conn = None
        log_ctx = self.log.bind(config_key=key)
        try:
            conn = self.db_pool.getconn()
            with conn.cursor() as cur:
                config_table_id = sql.Identifier(self.TABLE_NAME)
                upsert_sql = sql.SQL("""
                    INSERT INTO {config_table} (key, value, updated_at)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (key) DO UPDATE SET
                        value = EXCLUDED.value,
                        updated_at = EXCLUDED.updated_at;
                """).format(config_table=config_table_id)

                now_utc = datetime.now(timezone.utc)
                value_to_save = value.copy()
                if 'updated_at' not in value_to_save:
                     value_to_save['updated_at'] = now_utc.isoformat()

                try:
                    json_value = json.dumps(value_to_save)
                except TypeError as json_err:
                    log_ctx.error("Configuration value is not JSON serializable", error=str(json_err))
                    raise ValueError("Configuration value must be JSON serializable") from json_err

                cur.execute(upsert_sql, (key, json_value, now_utc))
                conn.commit()
                log_ctx.info("Configuration saved successfully")
        except Exception as e:
            if conn: conn.rollback()
            log_ctx.error("Failed to save configuration", error=str(e), exc_info=True)
            raise
        finally:
            if conn:
                self.db_pool.putconn(conn)

    def get_configuration(self, config_key: str) -> Optional[Dict[str, Any]]:
        """Retrieves a configuration value (JSONB) from the database."""
        conn = None
        log_ctx = self.log.bind(config_key=config_key)
        try:
            conn = self.db_pool.getconn()
            with conn.cursor(cursor_factory=extras.DictCursor) as cur:
                query = sql.SQL("SELECT value, updated_at FROM {config_table} WHERE key = %s LIMIT 1").format(
                    config_table=sql.Identifier(self.TABLE_NAME)
                )
                cur.execute(query, (config_key,))
                result = cur.fetchone()
                if result:
                    log_ctx.debug("Configuration retrieved", updated_at=result['updated_at'])
                    return result['value']
                else:
                    log_ctx.debug("Configuration key not found")
                    return None
        except Exception as e:
            log_ctx.error("Failed to get configuration", error=str(e), exc_info=True)
            return None
        finally:
            if conn:
                self.db_pool.putconn(conn)