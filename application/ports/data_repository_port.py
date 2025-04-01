from abc import ABC, abstractmethod
from typing import List, Dict

class DataRepositoryPort(ABC):
    @abstractmethod
    def save_data(self, data: List[Dict]) -> None:
        """
        Implements the logic to save transformed data to the repository.
        """
        pass

    @abstractmethod
    def save_data_upsert(self, data: List[Dict]) -> None:
        """
        Implements the logic to save transformed data using an upsert strategy.
        This should merge new records with existing ones to avoid duplicate key errors.
        """
        pass
