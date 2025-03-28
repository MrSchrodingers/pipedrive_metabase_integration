def process_pipedrive_data(etl_service):
    """
    Caso de uso que processa os dados do Pipedrive utilizando o serviço ETL.
    
    Parâmetros:
      etl_service (ETLService): instância do serviço de ETL.
    
    Retorna:
      Dados transformados processados pelo ETL.
    """
    result = etl_service.run_etl()
    return result
