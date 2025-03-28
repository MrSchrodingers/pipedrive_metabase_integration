from abc import ABC, abstractmethod

class DataRepositoryPort(ABC):
    @abstractmethod
    def save_data(self, data):
        """
        Deve implementar a lógica para salvar os dados transformados no repositório.
        """
        pass
