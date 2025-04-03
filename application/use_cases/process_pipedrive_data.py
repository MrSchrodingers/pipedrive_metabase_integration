from application.services.etl_service import ETLService
from typing import Dict

def run_pipedrive_etl_use_case(etl_service: ETLService) -> Dict[str, object]:
    """
    Use case that processes Pipedrive data using the ETL service.

    Parameters:
        etl_service (ETLService): instance of the ETL service.

    Returns:
        Dict: A dictionary containing the results of the ETL run.
    """
    result = etl_service.run_etl()
    return result