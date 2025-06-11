from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

import pandas as pd


class DestinationConnector(ABC):
    """Abstract base class for all destination connectors."""

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.resilience_manager: Optional[Any] = (
            None  # Will be ResilienceManager when configured
        )

    def _needs_resilience(self) -> bool:
        """Determine if this connector type requires resilience patterns."""
        class_name = self.__class__.__name__.lower()
        if "memory" in class_name or "mock" in class_name:
            return False
        return True

    def _configure_resilience(self, params: Dict[str, Any]) -> None:
        """Configure resilience patterns based on connector type."""
        if not self._needs_resilience():
            return

        from sqlflow.connectors.resilience import ResilienceManager
        from sqlflow.connectors.resilience_profiles import (
            get_resilience_config_for_connector,
        )

        resilience_config = params.get("resilience")
        if resilience_config is None:
            resilience_config = get_resilience_config_for_connector(
                self.__class__.__name__
            )

        if resilience_config is not None:
            self.resilience_manager = ResilienceManager(
                config=resilience_config, name=f"{self.__class__.__name__}_{id(self)}"
            )

    @abstractmethod
    def write(
        self,
        df: pd.DataFrame,
        options: Optional[Dict[str, Any]] = None,
        mode: str = "append",
        keys: Optional[List[str]] = None,
    ) -> None:
        """
        Write data to the destination using a specified mode.
        """
        raise NotImplementedError
