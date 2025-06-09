from typing import Optional


class ConnectionTestResult:
    """Result of a connection test."""

    def __init__(self, success: bool, message: Optional[str] = None):
        """Initialize a ConnectionTestResult.

        Args:
        ----
            success: Whether the test was successful
            message: Optional message with details

        """
        self.success = success
        self.message = message
