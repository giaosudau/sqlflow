"""Transaction management for DuckDB operations."""

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .engine import DuckDBEngine

logger = logging.getLogger(__name__)


class TransactionManager:
    """Manages transactions for DuckDB operations."""

    def __init__(self, engine: "DuckDBEngine"):
        """Initialize transaction manager.

        Args:
        ----
            engine: DuckDB engine instance

        """
        self.engine = engine
        self.in_transaction = False

    def __enter__(self):
        """Enter transaction context."""
        logger.debug("Starting transaction")
        self.in_transaction = True
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit transaction context.

        Args:
        ----
            exc_type: Exception type if raised
            exc_val: Exception value if raised
            exc_tb: Exception traceback if raised

        """
        if exc_type is None:
            logger.debug("Transaction completed successfully")
        else:
            logger.debug(f"Transaction failed: {exc_val}")

        self.in_transaction = False

    def commit(self):
        """Commit the current transaction."""
        if self.engine.connection:
            self.engine.connection.commit()
            logger.debug("Transaction committed")

    def rollback(self):
        """Rollback the current transaction."""
        if self.engine.connection:
            try:
                self.engine.connection.rollback()
                logger.debug("Transaction rolled back")
            except Exception as e:
                # Handle case where no transaction is active
                if "no transaction is active" in str(e).lower():
                    logger.debug("No active transaction to rollback")
                else:
                    logger.warning(f"Failed to rollback transaction: {e}")
