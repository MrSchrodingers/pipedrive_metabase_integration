import json
import time
from datetime import datetime, timedelta, timezone
import tracemalloc
from typing import Any, Dict, List, Optional
import structlog
from pydantic import ValidationError 
from tenacity import RetryError
import requests

from core_domain.value_objects.timestamp import Timestamp


from application.ports.pipedrive_client_port import PipedriveClientPort
from application.ports.data_repository_port import DataRepositoryPort
from infrastructure.repository_impl.lookup_repositories import (
    UserRepository, PersonRepository, StageRepository, PipelineRepository, OrganizationRepository
)
from application.mappers import deal_mapper
from application.schemas.deal_schema import DealSchema

from infrastructure.monitoring.metrics import (
    etl_counter, etl_failure_counter, etl_duration_hist,
    records_processed_counter, memory_usage_gauge, batch_size_gauge,
    db_operation_duration_hist, etl_empty_batches_total,
    etl_batch_validation_errors_total, etl_skipped_batches_total,
)

log = structlog.get_logger(__name__)

class ETLService:
    """
    Orchestrates the ETL process for Pipedrive deals, coordinating the API client,
    mappers, domain objects, data repository, and lookup repositories.
    """

    def __init__(
        self,
        client: PipedriveClientPort,
        data_repository: DataRepositoryPort,
        user_repository: UserRepository,
        person_repository: PersonRepository,
        stage_repository: StageRepository,
        pipeline_repository: PipelineRepository,
        org_repository: OrganizationRepository,
        mapper_module: Any = deal_mapper, 
        batch_size: int = 1000
    ):
        self.client = client
        self.data_repository = data_repository
        self.user_repo = user_repository
        self.person_repo = person_repository
        self.stage_repo = stage_repository
        self.pipeline_repo = pipeline_repository
        self.org_repo = org_repository
        self.mapper = mapper_module
        self.process_batch_size = batch_size
        self.log = log.bind(service="ETLService")

        try:
            self._custom_field_map = self.client.fetch_deal_fields_mapping()
            self._field_definitions = self._get_field_definitions_map()
            self._stage_id_to_col_map = self.data_repository.get_stage_id_to_column_map()
            self.log.info("ETL Service initialized with field maps and definitions.")
        except Exception as init_err:
             self.log.error("Failed to fetch initial mappings/definitions for ETL Service", error=str(init_err), exc_info=True)
             raise RuntimeError("ETL Service initialization failed: Could not fetch required mappings.") from init_err

    def _get_field_definitions_map(self) -> Dict[str, Dict]:
        """Fetches /dealFields and returns a map indexed by API hash key."""
        try:
             all_fields_list = self.client.fetch_deal_fields() 
             if not all_fields_list:
                  self.log.warning("Received empty list from fetch_deal_fields")
                  return {}
             return {field['key']: field for field in all_fields_list if 'key' in field}
        except AttributeError:
             self.log.error("PipedriveClientPort does not have fetch_deal_fields method.")
             self.log.warning("Falling back to empty field definitions map due to missing client method.")
             return {}
        except Exception as e:
             self.log.error("Failed to fetch or process deal field definitions", error=str(e), exc_info=True)
             return {} 


    def _enrich_persistence_dicts(self, persistence_dicts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Enriches persistence dictionaries with names fetched from lookup repositories.
        """
        if not persistence_dicts:
            return []

        enriched_dicts = [d.copy() for d in persistence_dicts] 
        enrich_start_time = time.monotonic()
        log_ctx = self.log.bind(batch_size=len(persistence_dicts))
        log_ctx.debug("Starting enrichment of persistence dictionaries.")

        try:
            # 1. Collect all unique IDs needed for lookups
            user_ids = set()
            person_ids = set()
            org_ids = set()
            stage_ids = set()
            pipeline_ids = set()

            for p_dict in enriched_dicts:
                 if p_dict.get("creator_user_id"): user_ids.add(p_dict["creator_user_id"])
                 if p_dict.get("owner_id"): user_ids.add(p_dict["owner_id"])
                 if p_dict.get("person_id"): person_ids.add(p_dict["person_id"])
                 if p_dict.get("org_id"): org_ids.add(p_dict["org_id"])
                 if p_dict.get("stage_id"): stage_ids.add(p_dict["stage_id"])
                 if p_dict.get("pipeline_id"): pipeline_ids.add(p_dict["pipeline_id"])

            # 2. Fetch name maps from lookup repositories concurrently? (For now, sequentially)
            user_map = self.user_repo.get_name_map_for_ids(user_ids) if user_ids else {}
            person_map = self.person_repo.get_name_map_for_ids(person_ids) if person_ids else {}
            org_map = self.org_repo.get_name_map_for_ids(org_ids) if org_ids else {}
            stage_map = self.stage_repo.get_name_map_for_ids(stage_ids) if stage_ids else {}
            pipeline_map = self.pipeline_repo.get_name_map_for_ids(pipeline_ids) if pipeline_ids else {}

            # 3. Apply names to the dictionaries
            for p_dict in enriched_dicts:
                 p_dict["creator_user_name"] = user_map.get(p_dict.get("creator_user_id"))
                 p_dict["owner_name"] = user_map.get(p_dict.get("owner_id"))
                 p_dict["person_name"] = person_map.get(p_dict.get("person_id"))
                 p_dict["org_name"] = org_map.get(p_dict.get("org_id"))
                 p_dict["stage_name"] = stage_map.get(p_dict.get("stage_id"))
                 p_dict["pipeline_name"] = pipeline_map.get(p_dict.get("pipeline_id"))

            duration = time.monotonic() - enrich_start_time
            log_ctx.debug("Enrichment completed.", duration_sec=f"{duration:.3f}s")
            return enriched_dicts

        except Exception as e:
             log_ctx.error("Failed during persistence dictionary enrichment", error=str(e), exc_info=True)
             return persistence_dicts 


    def run_etl(self, flow_type: str = "sync") -> Dict[str, Any]:
        """
        Executes the main ETL flow: Fetch, Map (API->Schema->Domain->Persistence), Enrich, Load.
        """
        run_start_time = time.monotonic()
        run_start_utc = datetime.now(timezone.utc)
        if not tracemalloc.is_tracing(): tracemalloc.start()
        etl_counter.labels(flow_type=flow_type).inc()
        run_log = self.log.bind(run_start_iso=run_start_utc.isoformat(), flow_type=flow_type)

        # Initialize results and counters
        result = {
            "status": "started", "total_fetched": 0, "total_schema_valid": 0,
            "total_domain_mapped": 0, "total_enriched": 0, "total_loaded": 0,
            "total_schema_failed": 0, "total_domain_failed": 0, "total_enrich_failed": 0,
            "total_load_failed": 0, "start_time": run_start_utc.isoformat(),
            "end_time": None, "duration_seconds": 0, "message": "ETL process initiated.",
            "peak_memory_mb": 0, "last_processed_timestamp": None
        }
        total_fetched = 0
        total_schema_valid = 0
        total_domain_mapped = 0
        total_loaded = 0
        total_schema_failed = 0
        total_domain_failed = 0
        total_load_failed = 0

        latest_update_time_in_run: Optional[datetime] = None
        run_log.info(f"Starting ETL run ({flow_type})")

        try:
            last_timestamp_str = self.client.get_last_timestamp()
            run_log.info("Fetching deals stream from Pipedrive", updated_since=last_timestamp_str)
            deal_stream_iterator = self.client.fetch_all_deals_stream(updated_since=last_timestamp_str)

            batch_num = 0
            records_for_processing_batch: List[Dict] = []
            run_log.info("Starting ETL batch processing loop", batch_size=self.process_batch_size)

            for api_record in deal_stream_iterator:
                total_fetched += 1
                records_for_processing_batch.append(api_record)

                update_time_str = api_record.get("update_time")
                if update_time_str:
                    try:
                        current_record_time = Timestamp(datetime.fromisoformat(update_time_str.replace('Z', '+00:00'))).value
                        if latest_update_time_in_run is None or current_record_time > latest_update_time_in_run:
                            latest_update_time_in_run = current_record_time
                    except Exception:
                         run_log.warning("Could not parse update_time from fetched record", record_id=api_record.get("id"), time_str=update_time_str)

                # --- Process Batch ---
                if len(records_for_processing_batch) >= self.process_batch_size:
                    batch_num += 1
                    api_batch = records_for_processing_batch
                    records_for_processing_batch = []
                    batch_log = run_log.bind(batch_num=batch_num, batch_size=len(api_batch))
                    batch_log.info("Processing ETL batch")
                    batch_start_time = time.monotonic()
                    batch_size_gauge.labels(flow_type=flow_type).set(len(api_batch))

                    processed_info = self._process_and_load_batch(
                        api_batch, batch_log, flow_type
                    )

                    # Update overall counters from batch results
                    total_schema_valid += processed_info["schema_valid"]
                    total_domain_mapped += processed_info["domain_mapped"]
                    total_loaded += processed_info["loaded"]
                    total_schema_failed += processed_info["schema_failed"]
                    total_domain_failed += processed_info["domain_failed"]
                    total_load_failed += processed_info["load_failed"]

                    batch_duration = time.monotonic() - batch_start_time
                    batch_log.info("ETL Batch processing complete", duration_sec=f"{batch_duration:.3f}s", **processed_info)

            # --- Process Final Partial Batch ---
            if records_for_processing_batch:
                batch_num += 1
                api_batch = records_for_processing_batch
                batch_log = run_log.bind(batch_num=batch_num, batch_size=len(api_batch))
                batch_log.info("Processing final ETL batch")
                batch_start_time = time.monotonic()
                batch_size_gauge.labels(flow_type=flow_type).set(len(api_batch))

                processed_info = self._process_and_load_batch(
                    api_batch, batch_log, flow_type
                )

                total_schema_valid += processed_info["schema_valid"]
                total_domain_mapped += processed_info["domain_mapped"]
                total_loaded += processed_info["loaded"]
                total_schema_failed += processed_info["schema_failed"]
                total_domain_failed += processed_info["domain_failed"]
                total_load_failed += processed_info["load_failed"]

                batch_duration = time.monotonic() - batch_start_time
                batch_log.info("Final ETL Batch processing complete", duration_sec=f"{batch_duration:.3f}s", **processed_info)

            # --- Finalization ---
            run_log.info("ETL stream processing finished.")
            result["status"] = "success" if total_load_failed == 0 and total_schema_failed == 0 and total_domain_failed == 0 else "completed_with_errors"
            result["message"] = f"ETL run {result['status']}."

            if latest_update_time_in_run and total_loaded > 0:
                try:
                    buffered_time = latest_update_time_in_run + timedelta(seconds=1)
                    iso_timestamp = buffered_time.strftime('%Y-%m-%dT%H:%M:%SZ')
                    self.client.update_last_timestamp(iso_timestamp)
                    result["last_processed_timestamp"] = iso_timestamp
                    run_log.info("Last processed timestamp updated in client/cache", timestamp=iso_timestamp)
                except Exception as cache_err:
                    run_log.error("Failed to update last timestamp", error=str(cache_err), exc_info=True)
            elif total_fetched > 0 and total_loaded == 0:
                 run_log.warning("ETL fetched records but loaded none. Last timestamp NOT updated.")
                 result["status"] = "error"
                 result["message"] = "ETL fetched records but loaded none."
            elif total_fetched == 0:
                 run_log.info("No new records fetched. Last timestamp not updated.")
                 result["status"] = "success_no_new_data"
                 result["message"] = "ETL completed successfully, no new data fetched."


        except (requests.exceptions.RequestException, RetryError) as api_err:
             run_log.critical("ETL failed due to API Client error", error=str(api_err), exc_info=True)
             result["status"] = "error"
             result["message"] = f"ETL failed: API Client Error - {api_err}"
             etl_failure_counter.labels(flow_type=flow_type).inc()
        except Exception as e:
            run_log.critical("Critical ETL failure during run_etl", error=str(e), exc_info=True)
            result["status"] = "error"
            result["message"] = f"Critical ETL failure: {e}"
            etl_failure_counter.labels(flow_type=flow_type).inc()
            result["total_load_failed"] = total_load_failed + (total_fetched - total_schema_valid - total_domain_failed - total_loaded)

        finally:
            run_end_time = time.monotonic()
            run_end_utc = datetime.now(timezone.utc)
            duration = run_end_time - run_start_time
            result["duration_seconds"] = round(duration, 3)
            result["end_time"] = run_end_utc.isoformat()
            etl_duration_hist.labels(flow_type=flow_type).observe(duration)

            # Update final counts in result
            result["total_fetched"] = total_fetched
            result["total_schema_valid"] = total_schema_valid
            result["total_domain_mapped"] = total_domain_mapped
            result["total_loaded"] = total_loaded
            result["total_schema_failed"] = total_schema_failed
            result["total_domain_failed"] = total_domain_failed
            result["total_load_failed"] = total_load_failed

            peak_mem_mb = 0
            if tracemalloc.is_tracing():
                try:
                    current_mem, peak_mem = tracemalloc.get_traced_memory()
                    peak_mem_mb = round(peak_mem / 1e6, 2)
                    tracemalloc.stop()
                    tracemalloc.clear_traces()
                    run_log.debug(f"Final Memory: Current={current_mem/1e6:.2f}MB, Peak={peak_mem_mb:.2f}MB")
                except Exception as trace_err:
                    run_log.error("Error stopping tracemalloc", error=str(trace_err))
                    if tracemalloc.is_tracing(): tracemalloc.stop() ; tracemalloc.clear_traces()
            result["peak_memory_mb"] = peak_mem_mb
            if peak_mem_mb > 0:
                 memory_usage_gauge.labels(flow_type=flow_type).set(peak_mem_mb)

            log_level = run_log.info if result["status"].startswith("success") else run_log.error
            log_level("ETL run summary", **result)
            print(f"ETL_COMPLETION_METRICS: {json.dumps(result)}")

            return result


    def _process_and_load_batch(
        self,
        api_batch: List[Dict],
        batch_log: structlog.BoundLoggerBase,
        flow_type: str
    ) -> Dict[str, int]:
        """Maps, enriches, and loads a single batch of data."""
        batch_results = {
            "schema_valid": 0, "domain_mapped": 0, "loaded": 0,
            "schema_failed": 0, "domain_failed": 0, "load_failed": 0
        }
        if not api_batch:
            etl_empty_batches_total.labels(flow_type=flow_type).inc()
            return batch_results

        # --- 1. API Dict -> Schema ---
        schema_batch: List[DealSchema] = []
        for api_record in api_batch:
            try:
                schema = self.mapper.map_api_dict_to_schema(api_record)
                schema_batch.append(schema)
            except ValidationError as schema_err:
                 batch_results["schema_failed"] += 1
                 etl_batch_validation_errors_total.labels(flow_type=flow_type, error_type="schema").inc()
                 batch_log.warning("Schema validation failed", record_id=api_record.get("id"), error=str(schema_err))
            except Exception as e:
                 batch_results["schema_failed"] += 1
                 etl_batch_validation_errors_total.labels(flow_type=flow_type, error_type="schema_unexpected").inc()
                 batch_log.error("Unexpected error during schema mapping", record_id=api_record.get("id"), error=str(e), exc_info=True)
        batch_results["schema_valid"] = len(schema_batch)

        if not schema_batch:
             batch_log.warning("No records passed schema validation in this batch.")
             etl_skipped_batches_total.labels(flow_type=flow_type).inc()
             return batch_results

        # --- 2. Schema -> Domain ---
        domain_batch: List[Any] = []
        for schema in schema_batch:
            try:
                domain_entity = self.mapper.map_schema_to_domain(schema, self._custom_field_map, self._field_definitions)
                domain_batch.append(domain_entity)
            except ValueError as domain_err:
                 batch_results["domain_failed"] += 1
                 etl_batch_validation_errors_total.labels(flow_type=flow_type, error_type="domain").inc()
                 batch_log.warning("Domain mapping failed", record_id=schema.id, error=str(domain_err))
            except Exception as e:
                 batch_results["domain_failed"] += 1
                 etl_batch_validation_errors_total.labels(flow_type=flow_type, error_type="domain_unexpected").inc()
                 batch_log.error("Unexpected error during domain mapping", record_id=schema.id, error=str(e), exc_info=True)
        batch_results["domain_mapped"] = len(domain_batch)

        if not domain_batch:
             batch_log.warning("No records passed domain mapping in this batch.")
             etl_skipped_batches_total.labels(flow_type=flow_type).inc()
             return batch_results

        # --- 3. Domain -> Persistence Dict ---
        persistence_batch: List[Dict[str, Any]] = []
        map_to_persist_failed = 0
        for domain_entity in domain_batch:
             try:
                 persistence_dict = self.mapper.map_domain_to_persistence_dict(domain_entity)
                 persistence_batch.append(persistence_dict)
             except Exception as e:
                  map_to_persist_failed += 1
                  batch_log.error("Failed mapping Domain to Persistence Dict", record_id=domain_entity.id, error=str(e), exc_info=True)
        batch_results["domain_failed"] += map_to_persist_failed
        batch_results["domain_mapped"] -= map_to_persist_failed 

        if not persistence_batch:
              batch_log.warning("No records successfully mapped to persistence dictionary.")
              etl_skipped_batches_total.labels(flow_type=flow_type).inc()
              return batch_results

        # --- 4. Enrich Persistence Dicts with Names ---
        enriched_batch = self._enrich_persistence_dicts(persistence_batch)

        # --- 5. Load to Repository ---
        if enriched_batch:
            try:
                with db_operation_duration_hist.labels(operation='upsert_deals').time():
                    self.data_repository.save_data_upsert(enriched_batch)
                batch_results["loaded"] = len(enriched_batch)
                records_processed_counter.labels(flow_type=flow_type).inc(len(enriched_batch))
            except Exception as load_err:
                batch_results["load_failed"] = len(enriched_batch)
                etl_failure_counter.labels(flow_type=flow_type).inc(len(enriched_batch))
                batch_log.error("Repository save_data_upsert failed for batch", error=str(load_err), record_count=len(enriched_batch), exc_info=True)
        else:
             batch_log.warning("No enriched records to load for this batch (enrichment or previous steps failed).")
             etl_skipped_batches_total.labels(flow_type=flow_type).inc()


        return batch_results


    def run_retroactive_backfill(self, deal_ids: List[str]) -> Dict[str, Any]:
        """
        Executes the stage history backfill process for a given list of Deal IDs.
        Fetches changelogs and updates the history columns in the data repository.
        """
        if not deal_ids:
            self.log.info("No deal IDs provided for retroactive backfill.")
            return {"status": "skipped", "processed_deals": 0, "updates_applied": 0, "api_errors": 0, "processing_errors": 0}

        run_start_time = time.monotonic()
        run_log = self.log.bind(flow_type="backfill", batch_deal_count=len(deal_ids))
        run_log.info("Starting retroactive stage history backfill run.")

        processed_deals_count = 0
        updates_to_apply: List[Dict[str, Any]] = []
        api_errors = 0
        processing_errors = 0
        db_update_errors = 0

        if not self._stage_id_to_col_map:
             run_log.error("Stage ID to history column map is empty. Cannot perform backfill.")
             return {"status": "error", "message": "Stage ID map is empty.", "processed_deals": 0, "updates_applied": 0, "api_errors": 0, "processing_errors": 1}

        for deal_id_str in deal_ids:
            deal_log = run_log.bind(deal_id=deal_id_str)
            try:
                deal_id = int(deal_id_str) 
                deal_log.debug("Fetching changelog for deal.")

                changelog = self.client.fetch_deal_changelog(deal_id)
                processed_deals_count += 1

                if not changelog:
                    deal_log.debug("No changelog entries found.")
                    continue

                stage_changes = []
                for entry in changelog:
                    if entry.get('field_key') == 'stage_id':
                        ts_str = entry.get('time') 
                        new_stage_id_val = entry.get('new_value')
                        if ts_str and new_stage_id_val is not None:
                             try:
                                 ts = datetime.strptime(ts_str, '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
                                 new_stage_id = int(new_stage_id_val)
                                 stage_changes.append({'timestamp': ts, 'stage_id': new_stage_id})
                             except (ValueError, TypeError):
                                 deal_log.warning("Could not parse stage change entry", entry=entry)
                                 processing_errors += 1

                if not stage_changes:
                    deal_log.debug("No valid 'stage_id' changes parsed from changelog.")
                    continue

                # Find the earliest timestamp for each stage ID encountered
                stage_changes.sort(key=lambda x: x['timestamp'])
                first_entry_times: Dict[int, datetime] = {}
                for change in stage_changes:
                    stage_id = change['stage_id']
                    timestamp = change['timestamp']
                    if stage_id not in first_entry_times:
                        first_entry_times[stage_id] = timestamp

                # Prepare updates for the database
                for stage_id, first_timestamp in first_entry_times.items():
                    column_name = self._stage_id_to_col_map.get(stage_id)
                    if column_name:
                        updates_to_apply.append({
                            'deal_id': deal_id_str, 
                            'stage_column': column_name,
                            'timestamp': first_timestamp 
                        })
                    else:
                        deal_log.warning("Stage ID from changelog not found in current stage map", stage_id=stage_id)
                        processing_errors += 1 

            except (requests.exceptions.RequestException, RetryError) as api_err:
                 api_errors += 1
                 deal_log.error("API error fetching changelog", error=str(api_err))
            except ValueError:
                 processing_errors += 1
                 deal_log.error("Invalid deal_id format", deal_id_str=deal_id_str)
            except Exception as e:
                 processing_errors += 1
                 deal_log.error("Error processing changelog", error=str(e), exc_info=True)

        # Apply collected updates to the database
        updates_applied_count = 0
        if updates_to_apply:
            run_log.info("Applying stage history updates to database", update_count=len(updates_to_apply))
            try:
                self.data_repository.update_stage_history(updates_to_apply)
                updates_applied_count = len(updates_to_apply)
            except Exception as db_err:
                db_update_errors += 1
                run_log.error("Failed during database update_stage_history", error=str(db_err), exc_info=True)
        else:
            run_log.info("No stage history updates generated from this batch.")

        duration = time.monotonic() - run_start_time
        final_status = "success"
        if api_errors > 0 or processing_errors > 0 or db_update_errors > 0:
             final_status = "completed_with_errors"
        run_log.info("Retroactive backfill run finished.", status=final_status, duration_sec=f"{duration:.3f}s")

        return {
            "status": final_status,
            "processed_deals": processed_deals_count,
            "updates_generated": len(updates_to_apply), 
            "updates_applied_attempted": updates_applied_count,
            "api_errors": api_errors,
            "processing_errors": processing_errors + db_update_errors, 
            "duration_seconds": round(duration, 3)
        }

    # --- run_etl_with_data method (kept for experiments) ---
    def run_etl_with_data(self, data: List[Dict], batch_size: int, flow_type: str = "experiment") -> Dict:
        """ Executes ETL using provided data list instead of API stream, for testing/experiments. """
        run_start_time = time.monotonic()
        run_start_utc = datetime.now(timezone.utc)
        if not tracemalloc.is_tracing(): tracemalloc.start()
        run_log = self.log.bind(run_start_iso=run_start_utc.isoformat(), flow_type=flow_type, batch_size=batch_size)
        run_log.info("Starting ETL run with provided data", data_size=len(data))

        # Initialize results and counters
        result = {
            "status": "started", "total_fetched": len(data), "total_schema_valid": 0,
            "total_domain_mapped": 0, "total_loaded": 0, "total_schema_failed": 0,
            "total_domain_failed": 0, "total_load_failed": 0, "start_time": run_start_utc.isoformat(),
            "end_time": None, "duration_seconds": 0, "message": "Experiment initiated.",
            "peak_memory_mb": 0, "success_rate": 0.0, "data_quality_issues": 0 
        }
        total_schema_valid = 0
        total_domain_mapped = 0
        total_loaded = 0
        total_schema_failed = 0
        total_domain_failed = 0
        total_load_failed = 0

        try:
            num_batches = (len(data) + batch_size - 1) // batch_size
            for i in range(num_batches):
                batch_start_idx = i * batch_size
                batch_end_idx = batch_start_idx + batch_size
                api_batch = data[batch_start_idx:batch_end_idx]

                batch_log = run_log.bind(batch_num=i+1, batch_size=len(api_batch))
                batch_log.info("Processing experiment batch")

                processed_info = self._process_and_load_batch(
                    api_batch, batch_log, flow_type
                )

                total_schema_valid += processed_info["schema_valid"]
                total_domain_mapped += processed_info["domain_mapped"]
                total_loaded += processed_info["loaded"]
                total_schema_failed += processed_info["schema_failed"]
                total_domain_failed += processed_info["domain_failed"]
                total_load_failed += processed_info["load_failed"]

            result["status"] = "success" if total_load_failed == 0 and total_schema_failed == 0 and total_domain_failed == 0 else "completed_with_errors"
            result["message"] = f"Experiment {result['status']}."
            if len(data) > 0:
                result["success_rate"] = total_loaded / len(data)

        except Exception as e:
             run_log.critical("Critical failure during experiment run", error=str(e), exc_info=True)
             result["status"] = "error"
             result["message"] = f"Critical experiment failure: {e}"
             result["total_load_failed"] = total_load_failed + (len(data) - total_loaded)

        finally:
            run_end_time = time.monotonic()
            run_end_utc = datetime.now(timezone.utc)
            duration = run_end_time - run_start_time
            result["duration_seconds"] = round(duration, 3)
            result["end_time"] = run_end_utc.isoformat()

            result["total_schema_valid"] = total_schema_valid
            result["total_domain_mapped"] = total_domain_mapped
            result["total_loaded"] = total_loaded
            result["total_schema_failed"] = total_schema_failed
            result["total_domain_failed"] = total_domain_failed
            result["total_load_failed"] = total_load_failed

            peak_mem_mb = 0
            if tracemalloc.is_tracing():
                try:
                    _, peak_mem = tracemalloc.get_traced_memory()
                    peak_mem_mb = round(peak_mem / 1e6, 2)
                    tracemalloc.stop(); tracemalloc.clear_traces()
                except Exception: pass
            result["peak_memory_mb"] = peak_mem_mb

            log_level = run_log.info if result["status"].startswith("success") else run_log.error
            log_level("Experiment run summary", **result)
            print(f"ETL_COMPLETION_METRICS: {json.dumps(result)}") 

            return result