import psutil
import time
from datetime import datetime, timedelta, timezone
import tracemalloc
from typing import Any, Dict, List, Tuple, Optional
import pandas as pd
import numpy as np
import requests
import structlog
import json
from pydantic import ValidationError
from tenacity import RetryError

from application.utils.column_utils import flatten_custom_fields, normalize_column_name
from infrastructure.monitoring.metrics import (
    etl_counter, 
    etl_failure_counter, 
    etl_duration_hist,
    records_processed_counter, 
    memory_usage_gauge, 
    batch_size_gauge,
    db_operation_duration_hist,
    transform_duration_summary,
    etl_empty_batches_total,
    etl_batch_validation_errors_total,
    etl_final_column_mismatch_total,
    etl_heartbeat,
    etl_cpu_usage_percent,
    etl_thread_count,
    etl_disk_usage_bytes,
    etl_skipped_batches_total,
    etl_last_successful_run_timestamp,
    etl_transformation_error_rate,
    etl_loaded_records_per_batch,
    hist_load,
    hist_extract,
    hist_transform
)
from application.ports.pipedrive_client_port import PipedriveClientPort
from application.ports.data_repository_port import DataRepositoryPort
from application.schemas.deal_schema import DealSchema
from infrastructure.repository_impl.pipedrive_repository import UNKNOWN_NAME

log = structlog.get_logger()

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
        self._stage_id_to_column_name_map = self.repository.get_stage_id_to_column_map()

        try:
             self._all_stages_details = self.client.fetch_all_stages_details()
             self._stage_id_to_normalized_name_map = self._build_stage_id_map(self._all_stages_details)
        except Exception as stage_err:
             self.log.error("Failed to fetch initial stage details during init.", error=str(stage_err))
             self._all_stages_details = []
             self._stage_id_to_normalized_name_map = {}
        
    def run_etl_with_data(self, data: List[Dict], batch_size: int, flow_type: str) -> Dict:
        self.log.warning("Running ETL with provided data for test/experiment.", data_size=len(data), batch_size=batch_size)
        original_fetch = self.client.fetch_all_deals_stream
        original_process_batch_size = self.process_batch_size

        def mock_stream(**kwargs):
            self.log.debug("Using mocked deal stream for test run.")
            yield from data

        self.client.fetch_all_deals_stream = mock_stream
        self.process_batch_size = batch_size
        result = self.run_etl(flow_type=flow_type)

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
        flow_type: str
    ) -> Tuple[List[Dict], int]:
        """Valida, transforma e enriquece um batch de deals usando lookups no DB."""
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
                transform_log.error("Unexpected error during Pydantic validation", record_id=record_id, exc_info=True)

        pydantic_valid_count = len(valid_input_for_df)
        transform_log = transform_log.bind(pydantic_valid_count=pydantic_valid_count, pydantic_failed_count=pydantic_failed_count)

        if not valid_input_for_df:
            transform_log.warning("No records passed Pydantic validation in the batch.")
            return [], pydantic_failed_count

        # 2. DataFrame e transformação
        validated_records: List[Dict] = []
        transform_failed_count = 0
        db_lookup_maps = {}
        
        try:
            df = pd.DataFrame(valid_input_for_df)
            
            # --- Coletar IDs para Lookup no DB ---
            user_ids_needed = set(df['creator_user_id'].dropna().unique()) | set(df['owner_id'].dropna().unique())
            df["owner_id_parsed"] = df["owner_id"].apply(lambda x: x.get("id") if isinstance(x, dict) else x).astype('Int64')
            owner_ids_needed = set(df['owner_id_parsed'].dropna().unique())
            user_ids_needed.update(owner_ids_needed) 

            person_ids_needed = set(df['person_id'].dropna().unique())
            stage_ids_needed = set(df['stage_id'].dropna().unique())
            pipeline_ids_needed = set(df['pipeline_id'].dropna().unique())
            df["org_id_parsed"] = df["org_id"].apply(lambda x: x.get("id") if isinstance(x, dict) else x).astype('Int64')
            org_ids_needed = set(df['org_id_parsed'].dropna().unique())


            transform_log.debug("IDs collected for DB lookup",
                                users=len(user_ids_needed), persons=len(person_ids_needed),
                                stages=len(stage_ids_needed), pipelines=len(pipeline_ids_needed),
                                orgs=len(org_ids_needed))

            # --- Buscar Mapas do DB para o Batch ---
            lookup_start_time = time.monotonic()
            try:
                db_lookup_maps = self.repository.get_lookup_maps_for_batch(
                    user_ids={int(uid) for uid in user_ids_needed if pd.notna(uid)}, 
                    person_ids={int(pid) for pid in person_ids_needed if pd.notna(pid)},
                    stage_ids={int(sid) for sid in stage_ids_needed if pd.notna(sid)},
                    pipeline_ids={int(plid) for plid in pipeline_ids_needed if pd.notna(plid)},
                    org_ids={int(oid) for oid in org_ids_needed if pd.notna(oid)}
                )
                lookup_duration = time.monotonic() - lookup_start_time
                transform_log.info("Fetched batch lookups from DB", duration_sec=f"{lookup_duration:.3f}s",
                                   users=len(db_lookup_maps.get('users', {})), persons=len(db_lookup_maps.get('persons', {})),
                                   stages=len(db_lookup_maps.get('stages', {})), pipelines=len(db_lookup_maps.get('pipelines', {})),
                                   orgs=len(db_lookup_maps.get('orgs', {})))
            except Exception as lookup_err:
                 transform_log.error("Failed to fetch lookups from DB for batch", error=str(lookup_err), exc_info=True)
                 db_lookup_maps = {'users': {}, 'persons': {}, 'stages': {}, 'pipelines': {}, 'orgs': {}}


            # --- Transformações usando os mapas buscados do DB ---
            transformed_df = pd.DataFrame()
            transformed_df["id"] = df["id"].astype(str)
            transformed_df["titulo"] = df["title"].fillna("").astype(str)
            status_map = { "won": "Ganho", "lost": "Perdido", "open": "Em aberto", "deleted": "Deletado" }
            transformed_df["status"] = df["status"].fillna("").astype(str).map(status_map).fillna(df["status"])
            transformed_df["currency"] = df["currency"].fillna("USD").astype(str)
            transformed_df["value"] = pd.to_numeric(df["value"], errors='coerce').fillna(0.0)
            transformed_df["add_time"] = pd.to_datetime(df["add_time"], errors='coerce', utc=True)
            transformed_df["update_time"] = pd.to_datetime(df["update_time"], errors='coerce', utc=True)

            # Mapeamentos usando db_lookup_maps
            batch_user_map = db_lookup_maps.get('users', {})
            batch_person_map = db_lookup_maps.get('persons', {})
            batch_stage_map = db_lookup_maps.get('stages', {}) 
            batch_pipeline_map = db_lookup_maps.get('pipelines', {})
            batch_org_map = db_lookup_maps.get('orgs', {})

            transformed_df["creator_user_id"] = pd.to_numeric(df["creator_user_id"], errors='coerce').astype('Int64')
            transformed_df['creator_user_name'] = transformed_df['creator_user_id'].map(batch_user_map).fillna(UNKNOWN_NAME)

            transformed_df["stage_id"] = pd.to_numeric(df["stage_id"], errors='coerce').astype('Int64')
            transformed_df['stage_name'] = transformed_df['stage_id'].map(batch_stage_map).fillna(UNKNOWN_NAME)

            transformed_df["pipeline_id"] = pd.to_numeric(df["pipeline_id"], errors='coerce').astype('Int64')
            transformed_df['pipeline_name'] = transformed_df['pipeline_id'].map(batch_pipeline_map).fillna(UNKNOWN_NAME)

            transformed_df["person_id"] = pd.to_numeric(df["person_id"], errors='coerce').astype('Int64')
            transformed_df['person_name'] = transformed_df['person_id'].map(batch_person_map).fillna(UNKNOWN_NAME)

            # Usar owner_id_parsed que já foi calculado
            transformed_df["owner_id"] = df["owner_id_parsed"] 
            transformed_df["owner_name"] = transformed_df["owner_id"].map(batch_user_map).fillna(UNKNOWN_NAME)

            # Usar org_id_parsed que já foi calculado
            transformed_df["org_id"] = df["org_id_parsed"] 
            transformed_df['org_name'] = transformed_df['org_id'].map(batch_org_map).fillna(UNKNOWN_NAME)

            # --- Outros campos base ---
            transformed_df["lost_reason"] = df["lost_reason"].fillna("").astype(str)
            transformed_df["visible_to"] = df["visible_to"].fillna("").astype(str)
            transformed_df["close_time"] = pd.to_datetime(df["close_time"], errors='coerce', utc=True)
            transformed_df["won_time"] = pd.to_datetime(df["won_time"], errors='coerce', utc=True)
            transformed_df["lost_time"] = pd.to_datetime(df["lost_time"], errors='coerce', utc=True)
            transformed_df["first_won_time"] = pd.to_datetime(
                df.get("first_won_time", pd.NaT), errors='coerce', utc=True
            )
            transformed_df["expected_close_date"] = pd.to_datetime(df["expected_close_date"], errors='coerce').dt.date
            transformed_df["probability"] = pd.to_numeric(df["probability"], errors='coerce')

            # --- Campos Customizados ---
            repo_custom_mapping = self.repository.custom_field_mapping
            if repo_custom_mapping and 'custom_fields' in df.columns:
                transform_log.warning("Flatten Custom Fields", custom_fields_after_flatten=repo_custom_mapping)
                df['custom_fields_parsed'] = df['custom_fields'].apply(
                    lambda x: json.loads(x) if isinstance(x, str) else (x if isinstance(x, dict) else {})
                )
                transform_log.warning("Flatten Custom Fields", custom_fields_before_flatten=repo_custom_mapping)

                custom_fields_flattened_df = pd.json_normalize(
                    df['custom_fields_parsed'].apply(
                        lambda x: flatten_custom_fields(x, repo_custom_mapping)
                    ).tolist()
                )

                # Garantir alinhamento
                custom_fields_flattened_df.index = df.index

                # Concat com df principal
                transformed_df = pd.concat([transformed_df, custom_fields_flattened_df], axis=1)
            
            # --- Selecionar e Ordenar Colunas Finais ---
            final_columns = self.repository._get_all_columns()
            existing_cols_in_df = list(transformed_df.columns)
            ordered_final_columns = [col for col in final_columns if col in existing_cols_in_df]

            # Adicionar colunas que faltam no DataFrame com None
            missing_final_cols = [col for col in final_columns if col not in existing_cols_in_df]
            if missing_final_cols:
                transform_log.debug("Columns defined in repository are missing in transformed DataFrame, adding as None.", missing_columns=missing_final_cols)
                missing_df = pd.DataFrame({col: [None] * len(transformed_df) for col in missing_final_cols})
                transformed_df = pd.concat([transformed_df, missing_df], axis=1)
                transformed_df = transformed_df.copy()
                ordered_final_columns.extend(missing_final_cols)

            # Verificar colunas extras
            extra_cols = [col for col in existing_cols_in_df if col not in ordered_final_columns and col not in missing_final_cols]
            if extra_cols:
                transform_log.warning("Columns created during transform but not in final repository schema (will be dropped)", extra_columns=extra_cols)
            try:
                if extra_cols:
                    transform_log.info("Attempting to update schema dynamically for extra columns detected.", columns=extra_cols)
                    self.repository.add_columns_to_main_table(extra_cols, inferred_from_df=transformed_df)
                    etl_final_column_mismatch_total.labels(flow_type=flow_type).inc()
            except Exception as schema_err:
                transform_log.error("Failed to update schema with new columns", error=str(schema_err), exc_info=True)        

            # Selecionar apenas as colunas finais na ordem definida
            # Remover colunas extras que não estão no schema atual
            transformed_df = transformed_df[[col for col in ordered_final_columns if col in transformed_df.columns]]

            # Verifica se há colunas extras após concatenação com campos customizados
            current_cols = set(transformed_df.columns)
            defined_cols = set(final_columns)
            extra_cols = current_cols - defined_cols

            if extra_cols:
                transform_log.warning("Detected extra columns not present in schema. Will add dynamically.", extra_columns=list(extra_cols))
                try:
                    self.repository.add_columns_to_main_table(list(extra_cols), inferred_from_df=transformed_df)
                    # Recarregar o schema após adicionar
                    final_columns = self.repository._get_all_columns()
                    ordered_final_columns = [col for col in final_columns if col in transformed_df.columns]
                except Exception as schema_err:
                    transform_log.error("Failed to dynamically update schema with extra columns", error=str(schema_err), exc_info=True)

            # --- Limpeza Final ---
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
            transform_log.error("Pandas transformation/enrichment failed", exc_info=True)
            raise e 

        duration = time.monotonic() - start_time
        transform_duration_summary.labels(flow_type=flow_type).observe(duration)
        total_failed_in_batch = pydantic_failed_count + transform_failed_count
        if original_count > 0:
            error_rate = total_failed_in_batch / original_count
            etl_transformation_error_rate.labels(flow_type=flow_type).set(error_rate)
        etl_batch_validation_errors_total.labels(flow_type=flow_type, error_type="pydantic").inc(pydantic_failed_count)
        etl_batch_validation_errors_total.labels(flow_type=flow_type, error_type="transform").inc(transform_failed_count)
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
        """Executa o processo ETL completo, agora sem buscar mapas na inicialização."""
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

            # --- Extração (Streaming) ---
            last_timestamp_str = self.client.get_last_timestamp()
            run_log.info("Fetching deals stream from Pipedrive", updated_since=last_timestamp_str)
            with hist_extract.labels(flow_type=flow_type).time():
                deal_stream_iterator = self.client.fetch_all_deals_stream(updated_since=last_timestamp_str)

            # --- Transformação e Carga (Batching) ---
            batch_num = 0
            records_for_processing_batch: List[Dict] = []
            run_log.info("Starting ETL batch processing loop", batch_size=self.process_batch_size)

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


                if len(records_for_processing_batch) >= self.process_batch_size:
                    batch_num += 1
                    batch_to_process = records_for_processing_batch
                    records_for_processing_batch = []
                    batch_log = run_log.bind(batch_num=batch_num, batch_size=len(batch_to_process))
                    batch_log.info("Processing ETL batch")
                    batch_start_time = time.monotonic()
                    batch_size_gauge.labels(flow_type=flow_type).set(len(batch_to_process))

                    try:
                        with hist_transform.labels(flow_type=flow_type).time():
                            validated_batch, failed_count_in_batch = self._validate_and_transform_batch_pandas(
                                batch=batch_to_process,
                                flow_type=flow_type
                            )
                        total_failed += failed_count_in_batch
                        total_validated += len(validated_batch)
                        
                        if not validated_batch:
                            etl_empty_batches_total.labels(flow_type=flow_type).inc()
                        else:
                            etl_skipped_batches_total.labels(flow_type=flow_type).inc()
                        if validated_batch:
                            load_start = time.monotonic()
                            try:
                                with db_operation_duration_hist.labels(operation='upsert').time():
                                    batch_log.warning("Sending batch to repository upsert.", first_ids=[rec.get("id") for rec in validated_batch[:5]])
                                    with hist_load.labels(flow_type=flow_type).time():
                                        self.repository.save_data_upsert(validated_batch)
                                current_loaded_count = len(validated_batch)
                                total_loaded += current_loaded_count
                                records_processed_counter.labels(flow_type=flow_type).inc(current_loaded_count)
                                etl_loaded_records_per_batch.labels(flow_type=flow_type).observe(current_loaded_count)
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
                    with hist_transform.labels(flow_type=flow_type).time():
                        validated_batch, failed_count_in_batch = self._validate_and_transform_batch_pandas(
                            batch=batch_to_process,
                            flow_type=flow_type
                        )
                    total_failed += failed_count_in_batch
                    total_validated += len(validated_batch)

                    if validated_batch:
                        load_start = time.monotonic()
                        try:
                            with db_operation_duration_hist.labels(operation='upsert').time():
                                batch_log.warning("Sending batch to repository upsert.", first_ids=[rec.get("id") for rec in validated_batch[:5]])
                                with hist_load.labels(flow_type=flow_type).time():
                                    self.repository.save_data_upsert(validated_batch)
                            current_loaded_count = len(validated_batch)
                            total_loaded += current_loaded_count
                            records_processed_counter.labels(flow_type=flow_type).inc(current_loaded_count)
                            etl_loaded_records_per_batch.labels(flow_type=flow_type).observe(current_loaded_count)
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
            run_log.critical("Critical ETL failure during run_etl", exc_info=True)
            result["status"] = "error"
            if result["message"] == "ETL process did not complete.":
                result["message"] = f"Critical ETL failure: {str(e)}"
            result["total_failed"] = max(total_failed, total_fetched - total_loaded) 


        finally:
            run_end_time = time.monotonic()
            run_end_utc = datetime.now(timezone.utc)
            etl_last_successful_run_timestamp.labels(flow_type=flow_type).set(int(run_end_utc.timestamp()))
            duration = run_end_time - run_start_time
            result["duration_seconds"] = round(duration, 3)
            result["end_time"] = run_end_utc.isoformat()
            etl_duration_hist.labels(flow_type=flow_type).observe(duration)
            peak_mem_mb = 0
            etl_heartbeat.labels(flow_type=flow_type).set_to_current_time()
            etl_cpu_usage_percent.labels(flow_type=flow_type).set(psutil.cpu_percent())
            etl_thread_count.labels(flow_type=flow_type).set(len(psutil.Process().threads()))
            etl_disk_usage_bytes.labels(mount_point='/').set(psutil.disk_usage('/').used)
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

        stage_column_map = self._stage_id_to_column_name_map
        if not stage_column_map:
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
                        ts = self._parse_changelog_timestamp(entry.get('time'))
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
                    normalized_name = stage_column_map.get(stage_id)
                    if normalized_name:
                        column_name = normalized_name  
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
                run_log.error("Error processing changelog for deal", deal_id=deal_id_str, exc_info=True)

        if updates_to_apply:
            run_log.info(
                    "Preparando para chamar update_stage_history",
                    total_updates=len(updates_to_apply)
                )
            try:
                self.repository.update_stage_history(updates_to_apply)
                run_log.info(
                    "update_stage_history foi chamado com sucesso",
                    affected_deals=processed_deals_count
                )
            except Exception as db_err:
                run_log.error(
                    "Falha durante a atualização do histórico de stages",
                    error=str(db_err),
                    exc_info=True
                )
                return {
                    "status": "error",
                    "message": f"Database update failed: {db_err}",
                    "processed_deals": processed_deals_count,
                    "updates_generated": len(updates_to_apply),
                    "api_errors": api_errors,
                    "processing_errors": processing_errors
                }
        else:
            run_log.info(
                "Nenhuma mudança de stage para aplicar no backfill",
                processed_deals=processed_deals_count
            )

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