import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from application.utils.data_transform import normalize_currency, normalize_date
from datetime import datetime, timezone
from infrastructure.monitoring.metrics import etl_counter, etl_failure_counter, etl_duration

logger = logging.getLogger(__name__)

def transform_record(record):
    try:
        if "value" in record:
            record["value"] = normalize_currency(str(record.get("value")))
        if "add_time" in record:
            record["add_time"] = normalize_date(record.get("add_time"))
        if "update_time" in record:
            record["update_time"] = normalize_date(record.get("update_time"))
        return record
    except Exception as e:
        logger.error("Erro na transformação do registro", exc_info=True)
        return record

class ETLService:
    def __init__(self, client, repository):
        self.client = client
        self.repository = repository

    def run_etl(self):
        etl_counter.inc()
        overall_start_time = datetime.now(timezone.utc)
        try:
            logger.info("Iniciando execução do ETL")
            updated_since = None
            deals = self.client.fetch_all_deals(updated_since=updated_since)

            if not deals:
                logger.info("Nenhuma atualização encontrada")
                return "Sem alterações."

            # Define opções para a pipeline do Beam
            options = PipelineOptions(
                runner='DirectRunner',
                job_name='pipedrive_etl'
            )

            transform_start_time = datetime.now(timezone.utc)

            with beam.Pipeline(options=options) as pipeline:
                transformed_deals = (
                    pipeline
                    | "Criar Coleção" >> beam.Create(deals)
                    | "Transformar Registros" >> beam.Map(transform_record)
                )
                result = (
                    transformed_deals
                    | "Agrupar em Lista" >> beam.combiners.ToList()
                )
                pipeline_result = pipeline.run()
                pipeline_result.wait_until_finish()

            latency = (datetime.now(timezone.utc) - transform_start_time).total_seconds()
            logger.info("Transformação concluída", extra={"latency_seconds": latency, "records": len(deals)})

            self.repository.save_data_bulk_copy(result)
            
            new_timestamp = datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
            self.client.update_last_timestamp(new_timestamp)
            logger.info("ETL finalizado com sucesso", extra={"records_processados": len(deals)})

            overall_duration = (datetime.now(timezone.utc) - overall_start_time).total_seconds()
            etl_duration.observe(overall_duration)
            return f"ETL executado com sucesso com {len(deals)} registros processados."
        except Exception as e:
            etl_failure_counter.inc()
            logger.error("Falha na execução do ETL", exc_info=True)
            raise
