from abc import ABC, abstractmethod
from typing import Any, Dict, Optional

import pandas as pd


class DestinationConnector(ABC):
    """Abstract base class for all destination connectors."""

    def __init__(self, config: Dict[str, Any]):
        self.config = config

    @abstractmethod
    def write(
        self,
        df: pd.DataFrame,
        options: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Write data to the destination.

        Args:
            df: The pandas DataFrame to write.
            options: A dictionary of options for the connector.
        """
        raise NotImplementedError
