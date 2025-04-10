import time
from datetime import datetime, timedelta, timezone
import tracemalloc
from typing import Any, Dict, List, Tuple, Optional, Set
import pandas as pd
import numpy as np
import requests
import structlog
import json
from pydantic import ValidationError
from tenacity import RetryError

from application.utils.batch_optimizer import DynamicBatchOptimizer
from application.utils.column_utils import normalize_column_name
from infrastructure.monitoring.metrics import (
    etl_counter, etl_failure_counter, etl_duration_hist,
    records_processed_counter, memory_usage_gauge, batch_size_gauge,
    db_operation_duration_hist,
    transform_duration_summary
)
from application.ports.pipedrive_client_port import PipedriveClientPort
from application.ports.data_repository_port import DataRepositoryPort
from application.schemas.deal_schema import DealSchema
from infrastructure.repository_impl.pipedrive_repository import BASE_COLUMNS as REPO_BASE_COLUMNS

log = structlog.get_logger()

UNKNOWN_NAME = "Desconhecido"
STAGE_HISTORY_COLUMN_PREFIX = "moved_to_stage_"

class ETLService:
    def __init__(
        self,
        client: PipedriveClientPort,
        repository: DataRepositoryPort,
        batch_size: int = 1000
    ):
        self.client = client
        self.repository = repository
        self.process_batch_size = batch_size
        self.log = log.bind(service="ETLService")
        self._all_stages_details = self.client.fetch_all_stages_details()
        self._stage_id_to_normalized_name_map = self._build_stage_id_map(self._all_stages_details)
        self.batch_optimizer = DynamicBatchOptimizer(config={
            'initial_size': batch_size,
            'max_size': 2000,
            'min_size': 500,
            'max_memory': 4096,
            'memory_threshold': 0.8,
            'reduce_factor': 0.5,
            'increase_factor': 1.1,
            'history_window': 10,
            'duration_threshold': 600
        })
        self._current_batch_size = batch_size
        # Mapas carregados no início do run_etl
        self._user_map: Dict[int, str] = {}
        self._pipeline_map: Dict[int, str] = {}
        self._person_map: Dict[int, str] = {}
        
    def run_etl_with_data(self, data: List[Dict], batch_size: int, flow_type: str) -> Dict:
        """Executa o ETL com dados fornecidos para fins de teste."""
        self.log.warning("Running ETL with provided data for test/experiment.", data_size=len(data), batch_size=batch_size)
        original_fetch = self.client.fetch_all_deals_stream
        original_process_batch_size = self.process_batch_size

        def mock_stream(**kwargs):
            self.log.debug("Using mocked deal stream for test run.")
            yield from data

        self.client.fetch_all_deals_stream = mock_stream
        self.process_batch_size = batch_size
        result = self.run_etl(flow_type=flow_type)

        # Restaurar estado original
        self.client.fetch_all_deals_stream = original_fetch
        self.process_batch_size = original_process_batch_size
        self.log.info("Restored original deal stream and batch size after test run.")
        return result

    def _build_stage_id_map(self, all_stages: List[Dict]) -> Dict[int, str]:
        """Cria um mapa de stage_id para nome normalizado."""
        id_map = {}
        if not all_stages:
            return {}
        for stage in all_stages:
            stage_id = stage.get('id')
            stage_name = stage.get('name')
            if stage_id and stage_name:
                try:
                    normalized = normalize_column_name(stage_name)
                    if normalized:
                        id_map[stage_id] = normalized
                except Exception as e:
                    self.log.warning("Failed to normalize stage name for map",
                                     stage_id=stage_id, name=stage_name, error=str(e))
        return id_map

    def _parse_changelog_timestamp(self, timestamp_str: Optional[str]) -> Optional[datetime]:
        """Converte o timestamp do changelog (ex: '2020-09-25 09:21:55') para datetime com UTC."""
        if not timestamp_str:
            return None
        try:
            dt = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')
            return dt.replace(tzinfo=timezone.utc)
        except (ValueError, TypeError):
            self.log.warning("Failed to parse changelog timestamp", timestamp_str=timestamp_str)
            return None

    def _validate_and_transform_batch_pandas(
        self,
        batch: List[Dict],
        user_map: Dict[int, str],        
        pipeline_map: Dict[int, str],     
        person_map: Dict[int, str],    
        flow_type: str
    ) -> Tuple[List[Dict], int]:
        """Valida, transforma e enriquece um batch de deals usando mapas pré-carregados."""
        start_time = time.monotonic()
        pydantic_failed_count = 0
        original_count = len(batch)
        transform_log = self.log.bind(batch_original_size=original_count, flow_type=flow_type)

        if not batch:
            return [], 0

        # 1. Validação com Pydantic
        valid_input_for_df = []
        for i, record in enumerate(batch):
            record_id = record.get("id", f"no-id-index-{i}")
            try:
                DealSchema.model_validate(record)
                valid_input_for_df.append(record)
            except ValidationError as e:
                pydantic_failed_count += 1
                errors_summary = [f"{err['loc']}: {err['msg']}" for err in e.errors()]
                transform_log.warning("Pydantic validation failed", record_id=record_id, errors=errors_summary)
            except Exception as e:
                pydantic_failed_count += 1
                transform_log.error("Unexpected error during Pydantic validation", record_id=record_id, error=str(e), exc_info=True)

        pydantic_valid_count = len(valid_input_for_df)
        transform_log = transform_log.bind(pydantic_valid_count=pydantic_valid_count, pydantic_failed_count=pydantic_failed_count)

        if not valid_input_for_df:
            transform_log.warning("No records passed Pydantic validation in the batch.")
            return [], pydantic_failed_count

        # 2. DataFrame e transformação
        validated_records: List[Dict] = []
        transform_failed_count = 0
        try:
            df = pd.DataFrame(valid_input_for_df)

            # Garantir colunas básicas
            required_cols = [
                col for col in REPO_BASE_COLUMNS
                if col not in ['creator_user_name', 'person_name', 'stage_name', 'pipeline_name', 'raw_data']
            ] + ['org_name']
            for col in required_cols:
                if col not in df.columns:
                    df[col] = pd.NA 

            transformed_df = pd.DataFrame()

            # --- IDs Básicos e Conversões ---
            transformed_df["id"] = df["id"].astype(str)
            transformed_df["titulo"] = df["title"].fillna("").astype(str)
            transformed_df["status"] = df["status"].fillna("").astype(str)
            status_map = { "won": "Ganho", "lost": "Perdido", "open": "Em aberto", "deleted": "Deletado" }
            transformed_df["status"] = transformed_df["status"].map(status_map).fillna(transformed_df["status"])
            transformed_df["currency"] = df["currency"].fillna("USD").astype(str)
            transformed_df["value"] = pd.to_numeric(df["value"], errors='coerce').fillna(0.0)
            transformed_df["add_time"] = pd.to_datetime(df["add_time"], errors='coerce', utc=True)
            transformed_df["update_time"] = pd.to_datetime(df["update_time"], errors='coerce', utc=True)

            # --- Mapeamento de Nomes para Users (usando mapa pré-carregado) ---
            transformed_df["creator_user_id"] = pd.to_numeric(df["creator_user_id"], errors='coerce').astype('Int64')
            transformed_df['creator_user_name'] = transformed_df['creator_user_id'].map(user_map).fillna(UNKNOWN_NAME)

            # --- Mapeamento de Stages (usando mapa normalizado interno) ---
            transformed_df["stage_id"] = pd.to_numeric(df["stage_id"], errors='coerce').astype('Int64')
            transformed_df['stage_name'] = transformed_df['stage_id'].map(self._stage_id_to_normalized_name_map).fillna(UNKNOWN_NAME)

            # --- Mapeamento de Pipelines (usando mapa pré-carregado) ---
            transformed_df["pipeline_id"] = pd.to_numeric(df["pipeline_id"], errors='coerce').astype('Int64')
            transformed_df['pipeline_name'] = transformed_df['pipeline_id'].map(pipeline_map).fillna(UNKNOWN_NAME)

            # --- Mapeamento de Persons (usando mapa pré-carregado) ---
            transformed_df["person_id"] = pd.to_numeric(df["person_id"], errors='coerce').astype('Int64')
            transformed_df['person_name'] = transformed_df['person_id'].map(person_map).fillna(UNKNOWN_NAME)

            # --- Mapeamento de Owner (usando mapa de usuários pré-carregado) ---
            df["owner_id_parsed"] = df["owner_id"].apply(lambda x: x.get("id") if isinstance(x, dict) else x)
            transformed_df["owner_id"] = pd.to_numeric(df["owner_id_parsed"], errors='coerce').astype('Int64')
            transformed_df["owner_name"] = transformed_df["owner_id"].map(user_map).fillna(UNKNOWN_NAME)

            # --- Mapeamento de Organization ---
            df["org_id_parsed"] = df["org_id"].apply(lambda x: x.get("id") if isinstance(x, dict) else x)
            transformed_df["org_id"] = pd.to_numeric(df["org_id_parsed"], errors='coerce').astype('Int64')
            transformed_df["org_name"] = df["org_name"].fillna(UNKNOWN_NAME).astype(str)

            # --- Outros campos base ---
            transformed_df["lost_reason"] = df["lost_reason"].fillna("").astype(str)
            transformed_df["visible_to"] = df["visible_to"].fillna("").astype(str) # Ajustar tipo se necessário
            transformed_df["close_time"] = pd.to_datetime(df["close_time"], errors='coerce', utc=True)
            transformed_df["won_time"] = pd.to_datetime(df["won_time"], errors='coerce', utc=True)
            transformed_df["lost_time"] = pd.to_datetime(df["lost_time"], errors='coerce', utc=True)
            transformed_df["first_won_time"] = pd.to_datetime(df["first_won_time"], errors='coerce', utc=True)
            transformed_df["expected_close_date"] = pd.to_datetime(df["expected_close_date"], errors='coerce').dt.date
            transformed_df["probability"] = pd.to_numeric(df["probability"], errors='coerce') 
            transformed_df["label"] = df["label"].fillna("").astype(str)


            # --- Campos Customizados ---
            repo_custom_mapping = self.repository.custom_field_mapping
            if repo_custom_mapping and 'custom_fields' in df.columns and not df['custom_fields'].isnull().all():
                df['custom_fields_parsed'] = df['custom_fields'].apply(
                    lambda x: json.loads(x) if isinstance(x, str) else (x if isinstance(x, dict) else {})
                )

                def extract_custom(row):
                    custom_data = {}
                    if isinstance(row, dict):
                        for api_key, normalized_name in repo_custom_mapping.items():
                            field_data = row.get(api_key)
                            if field_data is not None:
                                custom_data[normalized_name] = field_data
                            else:
                                custom_data[normalized_name] = None
                    # Garantir todas as colunas customizadas mapeadas
                    for normalized_name in repo_custom_mapping.values():
                        if normalized_name not in custom_data:
                            custom_data[normalized_name] = None
                    return pd.Series(custom_data)

                # Aplicar em 'custom_fields_parsed' que não são nulos
                custom_df = df.loc[df['custom_fields_parsed'].notna(), 'custom_fields_parsed'].apply(extract_custom)

                # Concatenar com segurança, evitando duplicatas de índice se houver
                if not custom_df.empty:
                     transformed_df = pd.concat([transformed_df, custom_df.reindex(transformed_df.index)], axis=1)


            # --- Selecionar e Ordenar Colunas Finais ---
            final_columns = self.repository._get_all_columns()
            existing_cols_in_df = list(transformed_df.columns)
            ordered_final_columns = [col for col in final_columns if col in existing_cols_in_df]

            # Adicionar colunas que faltam no DataFrame com None
            missing_final_cols = [col for col in final_columns if col not in existing_cols_in_df]
            if missing_final_cols:
                transform_log.debug("Columns defined in repository are missing in transformed DataFrame, adding as None.", missing_columns=missing_final_cols)
                for col in missing_final_cols:
                    transformed_df[col] = None 
                ordered_final_columns.extend(missing_final_cols)

            # Verificar colunas extras
            extra_cols = [col for col in existing_cols_in_df if col not in ordered_final_columns and col not in missing_final_cols]
            if extra_cols:
                transform_log.warning("Columns created during transform but not in final repository schema (will be dropped)", extra_columns=extra_cols)

            # Selecionar apenas as colunas finais na ordem definida
            transformed_df = transformed_df[ordered_final_columns]

            # --- Limpeza Final (sem alterações) ---
            # Substituir todos os tipos de nulos/NA por None para consistência JSON/DB
            transformed_df = transformed_df.replace({pd.NA: None, np.nan: None, pd.NaT: None})

            validated_records = transformed_df.to_dict('records')
            transform_succeeded_count = len(validated_records)
            transform_failed_count = pydantic_valid_count - transform_succeeded_count 

        except AttributeError as ae: 
            transform_failed_count = pydantic_valid_count
            transform_log.error("Pandas transformation failed due to AttributeError", error=str(ae), exc_info=True)
            raise ae 
        except KeyError as ke:
            transform_failed_count = pydantic_valid_count
            transform_log.error("Pandas transformation failed due to KeyError", error=str(ke), exc_info=True)
            raise ke
        except Exception as e:
            transform_failed_count = pydantic_valid_count
            transform_log.error("Pandas transformation/enrichment failed", error=str(e), exc_info=True)
            raise e 

        duration = time.monotonic() - start_time
        transform_duration_summary.labels(flow_type=flow_type).observe(duration)
        total_failed_in_batch = pydantic_failed_count + transform_failed_count
        transform_log.info(
            "Batch transformation completed",
            validated_count=len(validated_records),
            transform_errors=transform_failed_count,
            total_failed_in_batch=total_failed_in_batch,
            duration_sec=f"{duration:.3f}s"
        )
        return validated_records, total_failed_in_batch

    def _track_resources(self, flow_type: str) -> float:
        """Monitora o uso de memória e retorna a memória atual em bytes."""
        current_mem = 0.0
        if tracemalloc.is_tracing():
            try:
                current, peak = tracemalloc.get_traced_memory()
                memory_usage_gauge.labels(flow_type=flow_type).set(peak / 1e6)  # em MB
                self.log.debug(f"Memory Usage: Current={current/1e6:.2f}MB, Peak={peak/1e6:.2f}MB")
                current_mem = current
            except Exception as mem_err:
                self.log.warning("Failed to track memory usage", error=str(mem_err))
        else:
            self.log.debug("Tracemalloc is not running, skipping memory tracking.")
        return current_mem

    def run_etl(self, flow_type: str) -> Dict[str, object]:
        """Executa o processo ETL completo."""
        run_start_time = time.monotonic()
        run_start_utc = datetime.now(timezone.utc)
        if not tracemalloc.is_tracing():
             tracemalloc.start()
        etl_counter.labels(flow_type=flow_type).inc()

        result = {
            "status": "error", "total_fetched": 0, "total_validated": 0, "total_loaded": 0,
            "total_failed": 0, "start_time": run_start_utc.isoformat(), "end_time": None,
            "duration_seconds": 0, "message": "ETL process did not complete.", "peak_memory_mb": 0
        }
        latest_update_time_in_run: Optional[datetime] = None
        total_fetched = 0; total_validated = 0; total_loaded = 0; total_failed = 0
        run_log = self.log.bind(run_start_time=run_start_utc.isoformat(), flow_type=flow_type)

        try:
            run_log.info(f"Starting ETL run ({flow_type})")

            # --- Busca dos Mapas Auxiliares ---
            run_log.info("Fetching auxiliary data maps (Users, Stages, Pipelines, Persons)...")
            fetch_maps_start = time.monotonic()
            try:
                run_log.info("Fetching Users map...")
                self._user_map = self.client.fetch_all_users_map()
                run_log.info("Users map fetched.", count=len(self._user_map))

                run_log.info("Using pre-built normalized Stages map.", count=len(self._stage_id_to_normalized_name_map))
                normalized_stage_map = self._stage_id_to_normalized_name_map

                run_log.info("Fetching Pipelines map...")
                self._pipeline_map = self.client.fetch_all_pipelines_map()
                run_log.info("Pipelines map fetched.", count=len(self._pipeline_map))

                run_log.info("Fetching Persons map...")
                self._person_map = self.client.fetch_all_persons_map()
                run_log.info("Persons map fetched.", count=len(self._person_map))

                fetch_duration = time.monotonic() - fetch_maps_start
                run_log.info("Auxiliary data maps fetched successfully.", duration_sec=f"{fetch_duration:.3f}s")

                # Validar se mapas essenciais foram carregados
                if not self._user_map or not self._pipeline_map or not self._person_map or not normalized_stage_map:
                    run_log.critical("One or more essential auxiliary maps are empty. Aborting ETL.",
                                     users_count=len(self._user_map),
                                     pipelines_count=len(self._pipeline_map),
                                     persons_count=len(self._person_map),
                                     stages_count=len(normalized_stage_map))
                    raise ValueError("Essential auxiliary maps could not be loaded.")

            except Exception as map_err:
                run_log.critical("Critical error fetching auxiliary maps. Aborting ETL.", error=str(map_err), exc_info=True)
                result["message"] = f"Failed to fetch auxiliary maps: {map_err}"
                raise map_err 

            # --- Verificação de Schema ---
            run_log.info("Database schema assumed verified/created by Repository initialization.")

            # --- Extração (Streaming) ---
            last_timestamp_str = self.client.get_last_timestamp()
            run_log.info("Fetching deals stream from Pipedrive", updated_since=last_timestamp_str)
            deal_stream_iterator = self.client.fetch_all_deals_stream(updated_since=last_timestamp_str)

            # --- Transformação e Carga (Batching) ---
            batch_num = 0
            records_for_processing_batch: List[Dict] = []

            dynamic_batch_log = run_log.bind(
                initial_batch_size=self.process_batch_size,
                optimizer_config={
                    "min": self.batch_optimizer.min_size, "max": self.batch_optimizer.max_size
                }
            )
            dynamic_batch_log.info("Starting ETL batch processing loop", current_batch_size=self._current_batch_size)


            for deal_record in deal_stream_iterator:
                total_fetched += 1
                records_for_processing_batch.append(deal_record)

                update_time_str = deal_record.get("update_time")
                if update_time_str:
                    try:
                        current_record_time = pd.to_datetime(update_time_str, errors='coerce', utc=True)
                        if pd.notna(current_record_time):
                            if latest_update_time_in_run is None or current_record_time > latest_update_time_in_run:
                                latest_update_time_in_run = current_record_time
                        else:
                             run_log.warning("Could not parse update_time (Pandas)", record_id=deal_record.get("id"), time_str=update_time_str)
                    except Exception:
                        run_log.warning("Could not parse update_time from fetched record", record_id=deal_record.get("id"), time_str=update_time_str)


                if len(records_for_processing_batch) >= self._current_batch_size:
                    batch_num += 1
                    batch_to_process = records_for_processing_batch
                    records_for_processing_batch = [] 
                    batch_log = run_log.bind(batch_num=batch_num, batch_size=len(batch_to_process))
                    batch_log.info("Processing ETL batch")
                    batch_start_time = time.monotonic()
                    batch_size_gauge.labels(flow_type=flow_type).set(len(batch_to_process))

                    try:
                        validated_batch, failed_count_in_batch = self._validate_and_transform_batch_pandas(
                            batch=batch_to_process,
                            user_map=self._user_map,
                            pipeline_map=self._pipeline_map,
                            person_map=self._person_map,
                            flow_type=flow_type
                        )
                        total_failed += failed_count_in_batch
                        total_validated += len(validated_batch)

                        if validated_batch:
                            load_start = time.monotonic()
                            try:
                                with db_operation_duration_hist.labels(operation='upsert').time():
                                    self.repository.save_data_upsert(validated_batch)
                                current_loaded_count = len(validated_batch)
                                total_loaded += current_loaded_count
                                records_processed_counter.labels(flow_type=flow_type).inc(current_loaded_count)
                                load_duration = time.monotonic() - load_start
                                batch_log.info("ETL Batch loaded/upserted successfully", loaded_count=current_loaded_count,
                                               load_duration_sec=f"{load_duration:.3f}s")
                            except Exception as load_error:
                                etl_failure_counter.labels(flow_type=flow_type).inc(len(validated_batch))
                                failed_on_load = len(validated_batch)
                                total_failed += failed_on_load
                                total_validated -= failed_on_load
                                batch_log.error("Failed to load batch to repository", error=str(load_error),
                                                records_count=failed_on_load, exc_info=True)
                        else:
                            batch_log.warning("ETL Batch resulted in no validated records to load", failed_in_transform=failed_count_in_batch)

                    except Exception as batch_proc_err: 
                        batch_log.error("Critical error processing ETL batch, skipping.", error=str(batch_proc_err), exc_info=True)
                        failed_in_this_batch = len(batch_to_process)
                        etl_failure_counter.labels(flow_type=flow_type).inc(failed_in_this_batch)
                        total_failed += failed_in_this_batch

                    batch_duration = time.monotonic() - batch_start_time
                    batch_log.debug("ETL Batch processing complete", duration_sec=f"{batch_duration:.3f}s")

            if records_for_processing_batch:
                batch_num += 1
                batch_to_process = records_for_processing_batch
                batch_log = run_log.bind(batch_num=batch_num, batch_size=len(batch_to_process))
                batch_log.info("Processing final ETL batch")
                batch_start_time = time.monotonic()
                batch_size_gauge.labels(flow_type=flow_type).set(len(batch_to_process))

                try:
                    validated_batch, failed_count_in_batch = self._validate_and_transform_batch_pandas(
                         batch=batch_to_process,
                         user_map=self._user_map,
                         pipeline_map=self._pipeline_map,
                         person_map=self._person_map,
                         flow_type=flow_type
                    )
                    total_failed += failed_count_in_batch
                    total_validated += len(validated_batch)

                    if validated_batch:
                        load_start = time.monotonic()
                        try:
                            with db_operation_duration_hist.labels(operation='upsert').time():
                                self.repository.save_data_upsert(validated_batch)
                            current_loaded_count = len(validated_batch)
                            total_loaded += current_loaded_count
                            records_processed_counter.labels(flow_type=flow_type).inc(current_loaded_count)
                            load_duration = time.monotonic() - load_start
                            batch_log.info("Final ETL Batch loaded/upserted successfully", loaded_count=current_loaded_count,
                                           load_duration_sec=f"{load_duration:.3f}s")
                        except Exception as load_error:
                            etl_failure_counter.labels(flow_type=flow_type).inc(len(validated_batch))
                            failed_on_load = len(validated_batch)
                            total_failed += failed_on_load
                            total_validated -= failed_on_load
                            batch_log.error("Failed to load final batch to repository", error=str(load_error),
                                            records_count=failed_on_load, exc_info=True)
                    else:
                        batch_log.warning("Final ETL Batch resulted in no validated records to load", failed_in_transform=failed_count_in_batch)

                except Exception as batch_proc_err:
                    batch_log.error("Critical error processing final ETL batch, skipping.", error=str(batch_proc_err), exc_info=True)
                    failed_in_this_batch = len(batch_to_process)
                    etl_failure_counter.labels(flow_type=flow_type).inc(failed_in_this_batch)
                    total_failed += failed_in_this_batch

                batch_duration = time.monotonic() - batch_start_time
                batch_log.debug("Final ETL Batch processing complete", duration_sec=f"{batch_duration:.3f}s")


            # --- Finalização ---
            run_log.info("ETL stream processing finished.")
            if latest_update_time_in_run and total_loaded > 0: 
                try:
                    buffered_time = latest_update_time_in_run + timedelta(seconds=1)
                    iso_timestamp = buffered_time.strftime('%Y-%m-%dT%H:%M:%SZ')
                    self.client.update_last_timestamp(iso_timestamp)
                    run_log.info("Last timestamp updated in cache", timestamp=iso_timestamp)
                except Exception as cache_err:
                    run_log.error("Failed to update last timestamp in cache", error=str(cache_err), exc_info=True)
            elif total_fetched == 0:
                run_log.info("No new records fetched since last run. Last timestamp not updated.")
            elif total_loaded == 0 and total_fetched > 0:
                 run_log.warning("ETL fetched records but loaded none. Last timestamp NOT updated.",
                                fetched=total_fetched, loaded=total_loaded, failed=total_failed)
            else: 
                run_log.warning("ETL finished but could not determine or save the last timestamp reliably. Timestamp NOT updated.",
                                fetched=total_fetched, loaded=total_loaded, failed=total_failed, last_time=latest_update_time_in_run)

            result.update({
                "status": "success", "total_fetched": total_fetched, "total_validated": total_validated,
                "total_loaded": total_loaded, "total_failed": total_failed,
                "message": f"ETL completed. Fetched={total_fetched}, Validated={total_validated}, Loaded={total_loaded}, Failed={total_failed}."
            })

        except Exception as e:
            if result.get("status") != "error": 
                 etl_failure_counter.labels(flow_type=flow_type).inc()
            run_log.critical("Critical ETL failure during run_etl", error=str(e), exc_info=True)
            result["status"] = "error"
            if result["message"] == "ETL process did not complete.":
                result["message"] = f"Critical ETL failure: {str(e)}"
            result["total_failed"] = max(total_failed, total_fetched - total_loaded) 


        finally:
            run_end_time = time.monotonic()
            run_end_utc = datetime.now(timezone.utc)
            duration = run_end_time - run_start_time
            result["duration_seconds"] = round(duration, 3)
            result["end_time"] = run_end_utc.isoformat()
            etl_duration_hist.labels(flow_type=flow_type).observe(duration)
            peak_mem_mb = 0
            if tracemalloc.is_tracing():
                try:
                    current_mem, peak_mem = tracemalloc.get_traced_memory()
                    peak_mem_mb = round(peak_mem / 1e6, 2)
                    tracemalloc.stop()
                    run_log.debug(f"Final Memory Usage: Current={current_mem/1e6:.2f}MB, Peak={peak_mem_mb:.2f}MB")
                except Exception as trace_err:
                    run_log.error("Error stopping tracemalloc", error=str(trace_err))
                    if tracemalloc.is_tracing(): tracemalloc.stop()

            result["peak_memory_mb"] = peak_mem_mb

            # Reafirmar contagens finais
            result["total_fetched"] = total_fetched
            result["total_validated"] = total_validated 
            result["total_loaded"] = total_loaded     
            result["total_failed"] = total_failed     

            log_level = run_log.info if result["status"] == "success" else run_log.error
            log_level("ETL run summary", **result)
            try:
                print(f"ETL_COMPLETION_METRICS: {json.dumps(result)}")
            except TypeError: 
                print(f"ETL_COMPLETION_METRICS: {str(result)}")

            self._user_map = {}
            self._pipeline_map = {}
            self._person_map = {}
            self.log.debug("Cleared internal maps after ETL run.")

            return result

    def run_retroactive_backfill(self, deal_ids: List[str]) -> Dict[str, Any]:
        """
        Executa o backfill do histórico de stages para uma lista de deal IDs (Fluxo 2).
        """
        if not deal_ids:
            self.log.info("No deal IDs provided for retroactive backfill.")
            return {"status": "skipped", "processed_deals": 0, "updates_generated": 0}

        run_start_time = time.monotonic()
        run_log = self.log.bind(flow_type="backfill", batch_deal_count=len(deal_ids))
        run_log.info("Starting retroactive stage history backfill run.")

        processed_deals_count = 0
        updates_to_apply: List[Dict[str, Any]] = []
        api_errors = 0
        processing_errors = 0

        stage_id_map = self._stage_id_to_normalized_name_map
        if not stage_id_map:
            run_log.error("Stage ID to normalized name map is empty. Cannot perform backfill.")
            return {"status": "error", "message": "Stage ID map is empty."}

        for deal_id_str in deal_ids:
            try:
                deal_id = int(deal_id_str)
                deal_log = run_log.bind(deal_id=deal_id)
                deal_log.debug("Fetching changelog for deal.")

                changelog = self.client.fetch_deal_changelog(deal_id)
                processed_deals_count += 1

                if not changelog:
                    deal_log.debug("No changelog entries found for deal.")
                    continue

                stage_changes = []
                for entry in changelog:
                    if entry.get('field_key') == 'stage_id':
                        ts = self._parse_changelog_timestamp(entry.get('log_time') or entry.get('timestamp'))
                        new_stage_id = entry.get('new_value')
                        if ts and new_stage_id is not None:
                            try:
                                new_stage_id_int = int(new_stage_id)
                                stage_changes.append({'timestamp': ts, 'stage_id': new_stage_id_int})
                            except (ValueError, TypeError):
                                deal_log.warning("Could not parse new_value as int for stage_id change", entry=entry)

                if not stage_changes:
                    deal_log.debug("No 'stage_id' changes found in changelog.")
                    continue

                stage_changes.sort(key=lambda x: x['timestamp'])
                first_entry_times: Dict[int, datetime] = {}
                for change in stage_changes:
                    stage_id = change['stage_id']
                    timestamp = change['timestamp']
                    if stage_id not in first_entry_times:
                        first_entry_times[stage_id] = timestamp

                for stage_id, first_timestamp in first_entry_times.items():
                    normalized_name = stage_id_map.get(stage_id)
                    if normalized_name:
                        column_name = f"{STAGE_HISTORY_COLUMN_PREFIX}{normalized_name}"
                        updates_to_apply.append({
                            'deal_id': deal_id_str,
                            'stage_column': column_name,
                            'timestamp': first_timestamp
                        })
                    else:
                        deal_log.warning("Stage ID from changelog not found in current stage map", stage_id=stage_id)
            except RetryError as retry_err:
                api_errors += 1
                run_log.error("API RetryError fetching changelog", deal_id=deal_id_str, error=str(retry_err.last_attempt.exception()))
            except requests.exceptions.RequestException as req_err:
                api_errors += 1
                run_log.error("API RequestException fetching changelog", deal_id=deal_id_str, error=str(req_err))
            except ValueError:
                processing_errors += 1
                run_log.error("Invalid deal_id format, skipping", deal_id_str=deal_id_str)
            except Exception as e:
                processing_errors += 1
                run_log.error("Error processing changelog for deal", deal_id=deal_id_str, error=str(e), exc_info=True)

        if updates_to_apply:
            run_log.info(f"Applying {len(updates_to_apply)} stage history updates.")
            try:
                self.repository.update_stage_history(updates_to_apply)
            except Exception as db_err:
                run_log.error("Failed during database update for stage history", error=str(db_err), exc_info=True)
                return {
                    "status": "error",
                    "message": f"Database update failed: {db_err}",
                    "processed_deals": processed_deals_count,
                    "updates_generated": len(updates_to_apply),
                    "api_errors": api_errors,
                    "processing_errors": processing_errors
                }
        else:
            run_log.info("No stage history updates to apply for this batch.")

        duration = time.monotonic() - run_start_time
        status = "success" if api_errors == 0 and processing_errors == 0 else "partial_success"
        run_log.info("Retroactive backfill run finished.", status=status, duration_sec=f"{duration:.3f}s")
        return {
            "status": status,
            "processed_deals": processed_deals_count,
            "updates_generated": len(updates_to_apply),
            "api_errors": api_errors,
            "processing_errors": processing_errors,
            "duration_seconds": round(duration, 3)
        }
