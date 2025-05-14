"""Execution timer for debugging and performance analysis."""

import time
from contextlib import contextmanager
from typing import Dict, Generator, List, Optional


class ExecutionTimer:
    """Timer for tracking execution times of pipeline steps."""

    def __init__(self):
        """Initialize an ExecutionTimer."""
        self.timings: Dict[str, float] = {}
        self.start_times: Dict[str, float] = {}
        self.current_timers: List[str] = []

    def start(self, name: str) -> None:
        """Start a timer.

        Args:
            name: Name of the timer
        """
        self.start_times[name] = time.time()
        self.current_timers.append(name)

    def stop(self, name: Optional[str] = None) -> float:
        """Stop a timer and record the elapsed time.

        Args:
            name: Name of the timer, or None to stop the most recent timer

        Returns:
            Elapsed time in seconds

        Raises:
            ValueError: If the timer was not started or if name is not provided and no timers are active
        """
        if not self.current_timers:
            raise ValueError("No active timers")

        if name is None:
            name = self.current_timers[-1]
        elif name not in self.current_timers:
            raise ValueError(f"Timer '{name}' was not started")

        if name not in self.start_times:
            raise ValueError(f"Timer '{name}' was not started")

        elapsed = time.time() - self.start_times[name]
        self.timings[name] = elapsed
        self.current_timers.remove(name)
        return elapsed

    def get_timing(self, name: str) -> float:
        """Get the elapsed time for a timer.

        Args:
            name: Name of the timer

        Returns:
            Elapsed time in seconds

        Raises:
            ValueError: If the timer was not stopped
        """
        if name not in self.timings:
            raise ValueError(f"Timer '{name}' was not stopped")
        return self.timings[name]

    def get_all_timings(self) -> Dict[str, float]:
        """Get all timings.

        Returns:
            Dict mapping timer names to elapsed times
        """
        return self.timings.copy()

    def reset(self) -> None:
        """Reset all timers."""
        self.timings = {}
        self.start_times = {}
        self.current_timers = []

    @contextmanager
    def measure(self, name: str) -> Generator[None, None, None]:
        """Context manager for measuring execution time.

        Args:
            name: Name of the timer

        Yields:
            None
        """
        self.start(name)
        try:
            yield
        finally:
            self.stop(name)


timer = ExecutionTimer()


@contextmanager
def measure(name: str) -> Generator[None, None, None]:
    """Context manager for measuring execution time using the global timer.

    Args:
        name: Name of the timer

    Yields:
        None
    """
    with timer.measure(name):
        yield
