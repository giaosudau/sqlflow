from abc import ABC, abstractmethod
from typing import Any, Dict, Optional

import pandas as pd


class SourceConnector(ABC):
    """Abstract base class for all source connectors."""

    def __init__(self, config: Dict[str, Any]):
        self.config = config

    @abstractmethod
    def read(
        self,
        options: Optional[Dict[str, Any]] = None,
    ) -> pd.DataFrame:
        """
        Read data from the source.

        Args:
            options: A dictionary of options for the connector.

        Returns:
            A pandas DataFrame.
        """
        raise NotImplementedError
