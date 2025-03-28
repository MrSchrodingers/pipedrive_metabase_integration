from abc import ABC, abstractmethod

class PipedriveClientPort(ABC):
    @abstractmethod
    def fetch_data(self):
        """
        Deve implementar a lógica para buscar dados da API do Pipedrive.
        Retorna, por exemplo, uma lista de dicionários com os dados brutos.
        """
        pass
