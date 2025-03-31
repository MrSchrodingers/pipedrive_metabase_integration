import logging
import tracemalloc
from datetime import datetime, timezone
from typing import Dict, List, Generator
from prefect import get_run_logger

from infrastructure.monitoring.metrics import (
    etl_counter, etl_failure_counter, etl_duration,
    records_processed, memory_usage, batch_size, insert_duration
)
from application.ports.pipedrive_client_port import PipedriveClientPort
from application.ports.data_repository_port import DataRepositoryPort


class ETLService:
    def __init__(
        self,
        client: PipedriveClientPort,
        repository: DataRepositoryPort,
        batch_size: int = 5000
    ):
        self.client = client
        self.repository = repository
        self.batch_size = batch_size
        self.logger = get_run_logger()
        tracemalloc.start()

    def _transform_record(self, record: Dict) -> Dict:
        """Transformação segura com validação e tratamento de erros"""
        try:
            transformed = {
                "id": str(record.get("id", "")),
                "titulo": record.get("title", ""),
                "creator_user": record.get("creator_user_id"),
                "user_info": record.get("user_id"),
                "person_info": record.get("person_id"),
                "stage_id": record.get("stage_id"),
                "stage_name": (record.get("stage") or {}).get("name"),
                "pipeline_id": record.get("pipeline_id"),
                "pipeline_name": record.get("pipeline_name"),
                "status": record.get("status"),
                "value": float(record.get("value", 0)) / 100,
                "currency": record.get("currency", "USD"),
                "raw_data": record,
            }
            for time_field in ["add_time", "update_time"]:
                if record.get(time_field):
                    try:
                        transformed[time_field] = datetime.fromisoformat(
                            record[time_field].replace('Z', '+00:00'))
                    except ValueError:
                        transformed[time_field] = None

            # Add custom fields with type checking
            custom_fields = {}
            for k, v in record.get("custom_fields", {}).items():
                if k in self.repository.custom_field_mapping:
                    field_value = v.get("value") if isinstance(v, dict) else v
                    custom_fields[self.repository.custom_field_mapping[k]] = field_value or ""
            
            transformed.update(custom_fields)
            return transformed

        except Exception as e:
            self.logger.error(f"Erro na transformação do registro {record.get('id')}: {str(e)}")
            return None

    def _batch_generator(self, data: List[Dict]) -> Generator[List[Dict], None, None]:
        """Geração eficiente de lotes com controle de memória"""
        for i in range(0, len(data), self.batch_size):
            yield data[i:i + self.batch_size]

    def _track_resources(self):
        """Monitoramento detalhado de recursos"""
        current, peak = tracemalloc.get_traces_memory()
        memory_usage.set(peak)
        self.logger.debug(f"Uso de memória: Current={current/1e6:.2f}MB, Peak={peak/1e6:.2f}MB")

    def run_etl(self) -> Dict[str, object]:
        """Execução principal do ETL com gerenciamento completo de recursos"""
        etl_counter.inc()
        start_time = datetime.now(timezone.utc)
        result = {"status": "error", "processed": 0}

        try:
            # Fase de Extração
            self.logger.info("Iniciando extração de dados do Pipedrive")
            deals = self.client.fetch_all_deals()
            self.logger.info(f"Dados extraídos: {len(deals)} registros")

            # Processamento em lotes
            total_processed = 0
            for batch_num, batch in enumerate(self._batch_generator(deals), 1):
                batch_start = datetime.now(timezone.utc)
                batch_size.set(len(batch))

                # Transformação
                processed_batch = [rec for rec in (self._transform_record(r) for r in batch) if rec]
                
                # Carga
                if processed_batch:
                    with insert_duration.time():
                        self.repository.save_data_incremental(processed_batch)
                    
                    total_processed += len(processed_batch)
                    records_processed.inc(len(processed_batch))

                # Monitoramento
                self._track_resources()
                batch_duration = (datetime.now(timezone.utc) - batch_start).total_seconds()
                
                self.logger.info(
                    f"Lote {batch_num} processado | "
                    f"Sucesso: {len(processed_batch)}/{len(batch)} | "
                    f"Duração: {batch_duration:.2f}s | "
                    f"Memória: {tracemalloc.get_traced_memory()[1]/1e6:.2f}MB"
                )

            # Resultado final
            result.update({
                "status": "success",
                "processed": total_processed,
                "duration": (datetime.now(timezone.utc) - start_time).total_seconds(),
                "peak_memory": tracemalloc.get_traced_memory()[1]
            })

        except Exception as e:
            etl_failure_counter.inc()
            self.logger.error(f"Falha crítica no ETL: {str(e)}", exc_info=True)
            result["message"] = str(e)
        finally:
            tracemalloc.stop()
            etl_duration.observe(result["duration"])
            return result