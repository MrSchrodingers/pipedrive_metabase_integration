import time
from datetime import datetime, timezone
import tracemalloc
from typing import Dict, List, Tuple, Optional, Set
import pandas as pd
import numpy as np
import structlog
import json
from pydantic import ValidationError

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

    def _validate_and_transform_batch_pandas(
        self,
        batch: List[Dict],
        user_map: Dict[int, str],
        stage_map: Dict[int, str],
        pipeline_map: Dict[int, str]
    ) -> Tuple[List[Dict], int]:
        """Valida, transforma e enriquece um batch de deals, buscando nomes de persons sob demanda."""
        start_time = time.monotonic()
        pydantic_failed_count = 0
        original_count = len(batch)
        transform_log = self.log.bind(batch_original_size=original_count)

        if not batch:
            return [], 0

        # 1. Validação Pydantic 
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

        # 2. DataFrame e Transformação
        validated_records: List[Dict] = []
        transform_failed_count = 0
        try:
            df = pd.DataFrame(valid_input_for_df)
            required_cols = [
                col for col in REPO_BASE_COLUMNS
                if col not in ['creator_user_name', 'owner_user_name', 'person_name', 'stage_name', 'pipeline_name', 'raw_data']
            ]
            for col in required_cols:
                if col not in df.columns:
                    df[col] = pd.NA

            transformed_df = pd.DataFrame()

            # --- IDs Básicos e Conversões --- 
            transformed_df["id"] = df["id"].astype(str)
            transformed_df["titulo"] = df["title"].fillna("").astype(str)
            transformed_df["status"] = df["status"].fillna("").astype(str)
            transformed_df["currency"] = df["currency"].fillna("USD").astype(str)
            transformed_df["value"] = pd.to_numeric(df["value"], errors='coerce').fillna(0.0)
            transformed_df["add_time"] = pd.to_datetime(df["add_time"], errors='coerce', utc=True)
            transformed_df["update_time"] = pd.to_datetime(df["update_time"], errors='coerce', utc=True)

            # --- Mapeamento de Nomes (Users, Stages, Pipelines) ---
            transformed_df["creator_user_id"] = pd.to_numeric(df["creator_user_id"], errors='coerce').astype('Int64')
            transformed_df['creator_user_name'] = transformed_df['creator_user_id'].map(user_map).fillna(UNKNOWN_NAME)

            transformed_df["user_id"] = pd.to_numeric(df["user_id"], errors='coerce').astype('Int64') # Owner ID
            transformed_df['owner_user_name'] = transformed_df['user_id'].map(user_map).fillna(UNKNOWN_NAME)

            transformed_df["stage_id"] = pd.to_numeric(df["stage_id"], errors='coerce').astype('Int64')
            transformed_df['stage_name'] = transformed_df['stage_id'].map(stage_map).fillna(UNKNOWN_NAME)

            transformed_df["pipeline_id"] = pd.to_numeric(df["pipeline_id"], errors='coerce').astype('Int64')
            transformed_df['pipeline_name'] = transformed_df['pipeline_id'].map(pipeline_map).fillna(UNKNOWN_NAME)

            # --- Mapeamento de Persons (Sob Demanda) ---
            transformed_df["person_id"] = pd.to_numeric(df["person_id"], errors='coerce').astype('Int64')

            # Obter IDs únicos de persons neste batch (ignorando nulos)
            unique_person_ids_in_batch: Set[int] = set(transformed_df["person_id"].dropna().unique())

            batch_person_map = {}
            if unique_person_ids_in_batch:
                 transform_log.info(f"Found {len(unique_person_ids_in_batch)} unique person IDs in batch to fetch names for.")
                 fetch_names_start = time.monotonic()
                 try:
                     batch_person_map = self.client.fetch_person_names_for_ids(unique_person_ids_in_batch)
                     fetch_names_duration = time.monotonic() - fetch_names_start
                     transform_log.info(
                         f"Fetched {len(batch_person_map)} names for {len(unique_person_ids_in_batch)} unique person IDs.",
                         duration_sec=f"{fetch_names_duration:.3f}s"
                     )
                 except Exception as person_fetch_err:
                     transform_log.error(
                         "Failed to fetch person names for batch. Names will be 'Desconhecido'.",
                         error=str(person_fetch_err),
                         exc_info=True 
                     )
                     batch_person_map = {}


            # Usar o mapa específico do batch (pode estar vazio se a busca falhar ou não houver IDs)
            transformed_df['person_name'] = transformed_df['person_id'].map(batch_person_map).fillna(UNKNOWN_NAME)

            # --- Campos Customizados --- 
            repo_custom_mapping = self.repository.custom_field_mapping
            if repo_custom_mapping and 'custom_fields' in df.columns and not df['custom_fields'].isnull().all():
                def extract_custom(row):
                    custom_data = {}
                    if isinstance(row, dict):
                        for api_key, normalized_name in repo_custom_mapping.items():
                            field_data = row.get(api_key)
                            if isinstance(field_data, dict) and 'value' in field_data:
                                custom_data[normalized_name] = field_data.get("value")
                            elif isinstance(field_data, (str, int, float, bool)) or field_data is None:
                                custom_data[normalized_name] = field_data
                            elif isinstance(field_data, dict):
                                transform_log.debug("Custom field value is dict without 'value' key", api_key=api_key, value=field_data)
                                custom_data[normalized_name] = str(field_data) 
                            else:
                                transform_log.debug("Unexpected type for custom field value", api_key=api_key, type=type(field_data).__name__)
                                custom_data[normalized_name] = str(field_data) 

                    for normalized_name in repo_custom_mapping.values():
                        if normalized_name not in custom_data:
                             custom_data[normalized_name] = None 
                    return pd.Series(custom_data)

                custom_df = df.loc[df['custom_fields'].notna(), 'custom_fields'].apply(extract_custom)
                transformed_df = pd.concat([transformed_df, custom_df], axis=1)


            # --- Raw Data --- 
            original_record_map = {rec['id']: rec for rec in valid_input_for_df if 'id' in rec}
            try:
                map_ids = transformed_df['id'].astype(int)
            except ValueError:
                map_ids = pd.to_numeric(transformed_df['id'], errors='coerce')
                transform_log.warning("Non-numeric IDs found when mapping raw_data", invalid_ids=transformed_df.loc[map_ids.isna(), 'id'].tolist())
                map_ids = map_ids.astype('Int64')
            transformed_df['raw_data'] = map_ids.map(original_record_map)

            # --- Selecionar e Ordenar Colunas Finais ---
            final_columns = self.repository._get_all_columns()
            existing_cols_in_df = list(transformed_df.columns)
            ordered_final_columns = [col for col in final_columns if col in existing_cols_in_df]
            missing_final_cols = [col for col in final_columns if col not in existing_cols_in_df]
            if missing_final_cols:
                transform_log.warning("Columns defined in repository are missing in transformed DataFrame", missing_columns=missing_final_cols)
                for col in missing_final_cols:
                     transformed_df[col] = None
                     ordered_final_columns.append(col)

            extra_cols = [col for col in existing_cols_in_df if col not in ordered_final_columns]
            if extra_cols:
                transform_log.warning("Columns created during transform but not in final list (will be included)", extra_columns=extra_cols)
                ordered_final_columns.extend(extra_cols)

            transformed_df = transformed_df[ordered_final_columns]


            # --- Limpeza Final NaN/NaT (Nível Superior) ---
            transformed_df = transformed_df.replace({pd.NA: None, np.nan: None, pd.NaT: None})

            validated_records = transformed_df.to_dict('records')
            transform_succeeded_count = len(validated_records)
            transform_failed_count = pydantic_valid_count - transform_succeeded_count

        except AttributeError as ae: 
             transform_failed_count = pydantic_valid_count 
             transform_log.error("Pandas transformation failed due to AttributeError", error=str(ae), exc_info=True)
             raise ae
        except Exception as e:
            transform_failed_count = pydantic_valid_count 
            transform_log.error("Pandas transformation/enrichment failed", error=str(e), exc_info=True)
            return [], pydantic_failed_count + transform_failed_count

        duration = time.monotonic() - start_time
        transform_duration_summary.observe(duration)
        total_failed_in_batch = pydantic_failed_count + transform_failed_count
        transform_log.info( 
            "Batch transformation completed",
            validated_count=len(validated_records), transform_errors=transform_failed_count,
            total_failed_in_batch=total_failed_in_batch, duration_sec=f"{duration:.3f}s"
        )
        return validated_records, total_failed_in_batch

    def _track_resources(self):
        """Monitora uso de memória."""
        if tracemalloc.is_tracing():
            try:
                current, peak = tracemalloc.get_traced_memory()
                memory_usage_gauge.set(peak / 1e6) # Gauge em MB
                self.log.debug(f"Memory Usage: Current={current/1e6:.2f}MB, Peak={peak/1e6:.2f}MB")
            except Exception as mem_err:
                self.log.warning("Failed to track memory usage", error=str(mem_err))
        else:
            self.log.debug("Tracemalloc is not running, skipping memory tracking.")

    def run_etl(self) -> Dict[str, object]:
        """Executa o processo ETL completo."""
        run_start_time = time.monotonic()
        run_start_utc = datetime.now(timezone.utc)
        tracemalloc.start()
        etl_counter.inc()

        result = {
            "status": "error", "total_fetched": 0, "total_validated": 0, "total_loaded": 0,
            "total_failed": 0, "start_time": run_start_utc.isoformat(), "end_time": None,
            "duration_seconds": 0, "message": "ETL process did not complete.", "peak_memory_mb": 0
        }
        latest_update_time: Optional[datetime] = None
        total_fetched = 0; total_validated = 0; total_loaded = 0; total_failed = 0
        run_log = self.log.bind(run_start_time=run_start_utc.isoformat())

        try:
            run_log.info("Starting ETL run")

            # --- Busca dos Mapas Auxiliares ---
            run_log.info("Fetching auxiliary data maps (Users, Stages, Pipelines)...")
            fetch_maps_start = time.monotonic()
            try:
                run_log.info("Fetching Users map...")
                user_map = self.client.fetch_all_users_map()
                run_log.info("Users map fetched.", count=len(user_map))

                run_log.info("Fetching Stages map...")
                stage_map = self.client.fetch_all_stages_map()
                run_log.info("Stages map fetched.", count=len(stage_map))

                run_log.info("Fetching Pipelines map...")
                pipeline_map = self.client.fetch_all_pipelines_map()
                run_log.info("Pipelines map fetched.", count=len(pipeline_map))

                fetch_duration = time.monotonic() - fetch_maps_start
                run_log.info(
                    "Auxiliary data maps fetched successfully.",
                    user_count=len(user_map), stage_count=len(stage_map),
                    pipeline_count=len(pipeline_map), 
                    duration_sec=f"{fetch_duration:.3f}s"
                )
                if not user_map or not stage_map or not pipeline_map:
                    run_log.warning("One or more critical auxiliary maps (Users, Stages, Pipelines) are empty. Enrichment may be incomplete.")

            except Exception as map_err:
                run_log.critical("Critical error fetching auxiliary maps. Aborting ETL.", error=str(map_err), exc_info=True)
                result["message"] = f"Failed to fetch auxiliary maps: {map_err}"
                raise map_err # Aborta o ETL

            # --- Verificação de Schema --- (mantido)
            self.repository.ensure_schema_exists()
            run_log.info("Database schema verified/created.")

            # --- Extração (Streaming) --- 
            last_timestamp = self.client.get_last_timestamp()
            run_log.info("Fetching deals stream from Pipedrive", updated_since=last_timestamp)
            deal_stream_iterator = self.client.fetch_all_deals_stream(updated_since=last_timestamp)


            # --- Transformação e Carga (Batching) ---
            batch_num = 0
            records_for_processing_batch: List[Dict] = []

            for deal_record in deal_stream_iterator:
                total_fetched += 1
                records_for_processing_batch.append(deal_record)

                if len(records_for_processing_batch) >= self.process_batch_size:
                    batch_num += 1
                    batch_to_process = records_for_processing_batch
                    records_for_processing_batch = []
                    batch_log = run_log.bind(batch_num=batch_num, batch_size=len(batch_to_process))
                    batch_log.info("Processing ETL batch")
                    batch_start_time = time.monotonic()
                    batch_size_gauge.set(len(batch_to_process))

                    try:
                        validated_batch, failed_count_in_batch = self._validate_and_transform_batch_pandas(
                            batch_to_process, user_map, stage_map, pipeline_map 
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
                                records_processed_counter.inc(current_loaded_count)
                                load_duration = time.monotonic() - load_start
                                batch_log.info("ETL Batch loaded successfully", loaded_count=current_loaded_count, load_duration_sec=f"{load_duration:.3f}s")
                                # Rastrear último timestamp
                                for record in validated_batch:
                                    update_time = record.get("update_time")
                                    if isinstance(update_time, datetime):
                                        if update_time.tzinfo is None: update_time = update_time.replace(tzinfo=timezone.utc)
                                        if latest_update_time is None or update_time > latest_update_time: latest_update_time = update_time
                            except Exception as load_error:
                                etl_failure_counter.inc(len(validated_batch))
                                total_failed += len(validated_batch)
                                batch_log.error("Failed to load batch to repository", error=str(load_error), records_count=len(validated_batch), exc_info=True)
                        else:
                            batch_log.warning("ETL Batch resulted in no validated records to load", failed_in_batch=failed_count_in_batch)

                    except Exception as batch_proc_err:
                         batch_log.error("Critical error processing ETL batch, skipping.", error=str(batch_proc_err), exc_info=True)
                         etl_failure_counter.inc(len(batch_to_process))
                         total_failed += len(batch_to_process) - failed_count_in_batch 

                    self._track_resources()
                    batch_duration = time.monotonic() - batch_start_time
                    batch_log.debug("ETL Batch processing complete", duration_sec=f"{batch_duration:.3f}s")

            # Processar o último batch parcial
            if records_for_processing_batch:
                batch_num += 1
                batch_to_process = records_for_processing_batch
                batch_log = run_log.bind(batch_num=batch_num, batch_size=len(batch_to_process))
                batch_log.info("Processing final ETL batch")
                batch_start_time = time.monotonic()
                batch_size_gauge.set(len(batch_to_process))
                try:
                    validated_batch, failed_count_in_batch = self._validate_and_transform_batch_pandas(batch_to_process, user_map, stage_map, pipeline_map) # person_map removido
                    total_failed += failed_count_in_batch
                    total_validated += len(validated_batch)
                    if validated_batch:
                        load_start = time.monotonic()
                        try:
                            with db_operation_duration_hist.labels(operation='upsert').time():
                                self.repository.save_data_upsert(validated_batch)
                            current_loaded_count = len(validated_batch)
                            total_loaded += current_loaded_count
                            records_processed_counter.inc(current_loaded_count)
                            load_duration = time.monotonic() - load_start
                            batch_log.info("Final ETL Batch loaded successfully", loaded_count=current_loaded_count, load_duration_sec=f"{load_duration:.3f}s")
                            # Rastrear timestamp 
                            for record in validated_batch:
                                update_time = record.get("update_time")
                                if isinstance(update_time, datetime):
                                    if update_time.tzinfo is None: update_time = update_time.replace(tzinfo=timezone.utc)
                                    if latest_update_time is None or update_time > latest_update_time: latest_update_time = update_time
                        except Exception as load_error:
                            etl_failure_counter.inc(len(validated_batch))
                            total_failed += len(validated_batch)
                            batch_log.error("Failed to load final batch to repository", error=str(load_error), records_count=len(validated_batch), exc_info=True)
                    else:
                        batch_log.warning("Final ETL Batch resulted in no validated records to load", failed_in_batch=failed_count_in_batch)
                except Exception as batch_proc_err:
                    batch_log.error("Critical error processing final ETL batch, skipping.", error=str(batch_proc_err), exc_info=True)
                    etl_failure_counter.inc(len(batch_to_process))
                    total_failed += len(batch_to_process) - failed_count_in_batch
                self._track_resources()
                batch_duration = time.monotonic() - batch_start_time
                batch_log.debug("Final ETL Batch processing complete", duration_sec=f"{batch_duration:.3f}s")

            # --- Finalização --- 
            run_log.info("ETL stream processing finished.")

            if latest_update_time and total_loaded > 0:
                 try:
                     iso_timestamp = latest_update_time.isoformat()
                     self.client.update_last_timestamp(iso_timestamp)
                     run_log.info("Last timestamp updated in cache", timestamp=iso_timestamp)
                 except Exception as cache_err:
                    run_log.error("Failed to update last timestamp in cache", error=str(cache_err), exc_info=True)
            elif total_fetched > 0 and total_loaded == 0:
                run_log.warning("ETL fetched records but loaded zero. Check logs for transformation or loading errors. Last timestamp NOT updated.")
            elif total_fetched == 0:
                 run_log.info("No new records fetched since last run. Last timestamp not updated.")
            else:
                 run_log.warning("Records were loaded, but unable to save the last timestamp. Timestamp NOT updated.")


            result.update({
                "status": "success", "total_fetched": total_fetched, "total_validated": total_validated,
                "total_loaded": total_loaded, "total_failed": total_failed,
                "message": f"ETL completed. Fetched={total_fetched}, Validated={total_validated}, Loaded={total_loaded}, Failed={total_failed}."
            })

        except Exception as e:
            etl_failure_counter.inc()
            run_log.critical("Critical ETL failure during run_etl", error=str(e), exc_info=True)
            result["status"] = "error"
            if result["message"] == "ETL process did not complete.": result["message"] = f"Critical ETL failure: {str(e)}"
            if total_failed == 0 and total_fetched > 0:
                 result["total_failed"] = total_fetched 
            elif total_failed > 0 and total_failed < total_fetched and result["status"] == "error":
                 result["total_failed"] = total_fetched

        finally:
            run_end_time = time.monotonic()
            run_end_utc = datetime.now(timezone.utc)
            duration = run_end_time - run_start_time
            result["duration_seconds"] = round(duration, 3) 
            result["end_time"] = run_end_utc.isoformat()
            etl_duration_hist.observe(duration)
            peak_mem_mb = 0
            if tracemalloc.is_tracing():
                try:
                    current_mem, peak_mem = tracemalloc.get_traced_memory()
                    peak_mem_mb = round(peak_mem / 1e6, 2) 
                    tracemalloc.stop()
                    run_log.debug(f"Final Memory Usage: Current={current_mem/1e6:.2f}MB, Peak={peak_mem_mb:.2f}MB")
                except Exception as trace_err: run_log.error("Error stopping tracemalloc", error=str(trace_err))
            result["peak_memory_mb"] = peak_mem_mb

            result["total_fetched"] = total_fetched
            result["total_validated"] = total_validated
            result["total_loaded"] = total_loaded
            if result["status"] == "error" and result.get("total_failed", 0) < total_fetched:
                 result["total_failed"] = total_fetched


            log_level = run_log.info if result["status"] == "success" else run_log.error
            log_level("ETL run summary", **result)
            try: print(f"ETL_COMPLETION_METRICS: {json.dumps(result)}")
            except TypeError: print(f"ETL_COMPLETION_METRICS: {result}")
            return result