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
            
    def _transform_batch(self, batch: List[Dict]) -> List[Dict]:
        """
        Transforma um lote de registros aplicando a transformação em cada registro.
        
        - Registros que falharem na transformação (ou seja, quando _transform_record retornar None)
        serão ignorados.
        - Logs detalhados informam quantos registros foram transformados com sucesso e quantos falharam.
        
        Parâmetros:
        batch (List[Dict]): Lista de registros brutos a serem transformados.
        
        Retorna:
        List[Dict]: Lista dos registros transformados com sucesso.
        """
        transformed_records = []
        success_count = 0
        fail_count = 0
        
        for record in batch:
            try:
                transformed = self._transform_record(record)
                if transformed is not None:
                    transformed_records.append(transformed)
                    success_count += 1
                else:
                    fail_count += 1
                    self.logger.warning(f"Registro com id {record.get('id')} não foi transformado e será ignorado.")
            except Exception as e:
                fail_count += 1
                self.logger.error(f"Erro inesperado ao transformar o registro {record.get('id')}: {str(e)}", exc_info=True)
        
        self.logger.debug(f"Transformação do lote concluída: {success_count} registros transformados com sucesso, {fail_count} falharam.")
        return transformed_records

    def _track_resources(self):
        """Monitoramento detalhado de recursos"""
        current, peak = tracemalloc.get_traced_memory()
        memory_usage.set(peak)
        self.logger.debug(f"Uso de memória: Current={current/1e6:.2f}MB, Peak={peak/1e6:.2f}MB")

    def run_etl(self) -> Dict[str, object]:
        etl_counter.inc()
        start_time = datetime.now(timezone.utc)
        result = {"status": "error", "processed": 0}
        latest_update = None

        try:
            # Extração
            self.logger.info("Iniciando extração de dados do Pipedrive")
            deals = self.client.fetch_all_deals()
            self.logger.info(f"Dados extraídos: {len(deals)} registros")

            # Processamento em lotes
            total_processed = 0
            for batch_num, batch in enumerate(self._batch_generator(deals), 1):
                batch_start = datetime.now(timezone.utc)
                batch_size.set(len(batch))

                # Transformação
                processed_batch = self._transform_batch(batch)
                filtered_batch = self.repository.filter_existing_records(processed_batch)

                # Atualiza o último timestamp encontrado
                for rec in filtered_batch:
                    update_time = rec.get("update_time")
                    if update_time and (latest_update is None or update_time > latest_update):
                        latest_update = update_time

                # Carga usando a estratégia de staging e upsert
                if filtered_batch:
                    with insert_duration.time():
                        self.repository.save_data_upsert(filtered_batch)
                    total_processed += len(filtered_batch)
                    records_processed.inc(len(filtered_batch))

                # Monitoramento
                self._track_resources()
                batch_duration = (datetime.now(timezone.utc) - batch_start).total_seconds()
                self.logger.info(
                    f"Lote {batch_num} processado | Sucesso: {len(filtered_batch)}/{len(batch)} | "
                    f"Duração: {batch_duration:.2f}s | "
                    f"Memória: {tracemalloc.get_traced_memory()[1]/1e6:.2f}MB"
                )

            # Atualiza o last_update se um novo timestamp for encontrado
            if latest_update:
                self.client.update_last_timestamp(latest_update.isoformat())

            result.update({
                "status": "success",
                "processed": total_processed,
                "peak_memory": tracemalloc.get_traced_memory()[1]
            })

        except Exception as e:
            etl_failure_counter.inc()
            self.logger.error(f"Falha crítica no ETL: {str(e)}", exc_info=True)
            result["message"] = str(e)
        finally:
            duration = (datetime.now(timezone.utc) - start_time).total_seconds()
            result["duration"] = duration
            etl_duration.observe(duration)
            tracemalloc.stop()
            return result
