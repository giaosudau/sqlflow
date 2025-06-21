"""Functional programming utilities.

Common functional patterns for pipeline processing:
- Function composition
- Higher-order functions
- Immutable data transformations
- Pipeline-style operations

Following Raymond Hettinger's functional programming recommendations.
"""

from functools import reduce, wraps
from typing import Any, Callable, Dict, List, TypeVar

T = TypeVar("T")
U = TypeVar("U")
V = TypeVar("V")


def compose(*functions: Callable) -> Callable:
    """Compose functions right to left.

    Args:
        *functions: Functions to compose

    Returns:
        Composed function

    Examples:
        >>> add_one = lambda x: x + 1
        >>> multiply_two = lambda x: x * 2
        >>> composed = compose(add_one, multiply_two)
        >>> composed(3)  # add_one(multiply_two(3)) = add_one(6) = 7
        7
    """
    if not functions:
        return lambda x: x

    return reduce(lambda f, g: lambda x: f(g(x)), functions)


def pipe(value: T, *functions: Callable[[Any], Any]) -> Any:
    """Pipe value through functions left to right.

    Args:
        value: Initial value
        *functions: Functions to apply in sequence

    Returns:
        Final transformed value

    Examples:
        >>> pipe(3, lambda x: x * 2, lambda x: x + 1)
        7
    """
    return reduce(lambda v, f: f(v), functions, value)


def partial_dict(func: Callable, **kwargs: Any) -> Callable:
    """Create partial function with keyword arguments.

    Args:
        func: Function to partially apply
        **kwargs: Keyword arguments to fix

    Returns:
        Partial function

    Examples:
        >>> def greet(name, greeting="Hello"):
        ...     return f"{greeting}, {name}!"
        >>> say_hi = partial_dict(greet, greeting="Hi")
        >>> say_hi("World")
        'Hi, World!'
    """

    @wraps(func)
    def wrapper(*args, **extra_kwargs):
        combined_kwargs = {**kwargs, **extra_kwargs}
        return func(*args, **combined_kwargs)

    return wrapper


def map_dict(func: Callable[[Any], Any], data: Dict[str, Any]) -> Dict[str, Any]:
    """Apply function to all values in dictionary.

    Args:
        func: Function to apply to values
        data: Dictionary to transform

    Returns:
        New dictionary with transformed values

    Examples:
        >>> map_dict(str.upper, {"a": "hello", "b": "world"})
        {'a': 'HELLO', 'b': 'WORLD'}
    """
    return {key: func(value) for key, value in data.items()}


def filter_dict(
    predicate: Callable[[Any], bool], data: Dict[str, Any]
) -> Dict[str, Any]:
    """Filter dictionary values by predicate.

    Args:
        predicate: Function that returns True for values to keep
        data: Dictionary to filter

    Returns:
        New dictionary with filtered values

    Examples:
        >>> filter_dict(lambda x: isinstance(x, str), {"a": "hello", "b": 123})
        {'a': 'hello'}
    """
    return {key: value for key, value in data.items() if predicate(value)}


def flatten_dict(data: Dict[str, Any], separator: str = ".") -> Dict[str, Any]:
    """Flatten nested dictionary with dot notation.

    Args:
        data: Nested dictionary
        separator: Separator for nested keys

    Returns:
        Flattened dictionary

    Examples:
        >>> flatten_dict({"a": {"b": {"c": 1}}})
        {'a.b.c': 1}
    """

    def _flatten(obj: Any, prefix: str = "") -> Dict[str, Any]:
        if isinstance(obj, dict):
            result = {}
            for key, value in obj.items():
                new_key = f"{prefix}{separator}{key}" if prefix else key
                result.update(_flatten(value, new_key))
            return result
        else:
            return {prefix: obj}

    return _flatten(data)


def safe_get(
    data: Dict[str, Any], key_path: str, default: Any = None, separator: str = "."
) -> Any:
    """Safely get nested value from dictionary using dot notation.

    Args:
        data: Dictionary to query
        key_path: Dot-separated path to value
        default: Default value if path not found
        separator: Path separator

    Returns:
        Value at path or default

    Examples:
        >>> safe_get({"a": {"b": {"c": 1}}}, "a.b.c")
        1
        >>> safe_get({"a": {"b": {}}}, "a.b.c", "not found")
        'not found'
    """
    keys = key_path.split(separator)
    current = data

    for key in keys:
        if isinstance(current, dict) and key in current:
            current = current[key]
        else:
            return default

    return current


def chunk_list(data: List[T], chunk_size: int) -> List[List[T]]:
    """Split list into chunks of specified size.

    Args:
        data: List to chunk
        chunk_size: Size of each chunk

    Returns:
        List of chunks

    Examples:
        >>> chunk_list([1, 2, 3, 4, 5], 2)
        [[1, 2], [3, 4], [5]]
    """
    if chunk_size <= 0:
        raise ValueError("Chunk size must be positive")

    return [data[i : i + chunk_size] for i in range(0, len(data), chunk_size)]


def merge_dicts(*dicts: Dict[str, Any]) -> Dict[str, Any]:
    """Merge multiple dictionaries with later values overriding earlier ones.

    Args:
        *dicts: Dictionaries to merge

    Returns:
        Merged dictionary

    Examples:
        >>> merge_dicts({"a": 1}, {"b": 2}, {"a": 3})
        {'a': 3, 'b': 2}
    """
    result = {}
    for d in dicts:
        if isinstance(d, dict):
            result.update(d)
    return result


def with_timeout(timeout_seconds: float):
    """Decorator to add timeout to function execution.

    Args:
        timeout_seconds: Maximum execution time

    Returns:
        Decorator function
    """

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            import signal

            def timeout_handler(signum, frame):
                raise TimeoutError(
                    f"Function {func.__name__} timed out after {timeout_seconds}s"
                )

            old_handler = signal.signal(signal.SIGALRM, timeout_handler)
            signal.alarm(int(timeout_seconds))

            try:
                result = func(*args, **kwargs)
                return result
            finally:
                signal.alarm(0)
                signal.signal(signal.SIGALRM, old_handler)

        return wrapper

    return decorator


def memoize(func: Callable) -> Callable:
    """Simple memoization decorator for pure functions.

    Args:
        func: Function to memoize

    Returns:
        Memoized function
    """
    cache = {}

    @wraps(func)
    def wrapper(*args, **kwargs):
        # Create cache key from arguments
        key = str(args) + str(sorted(kwargs.items()))

        if key not in cache:
            cache[key] = func(*args, **kwargs)

        return cache[key]

    wrapper.cache_clear = lambda: cache.clear()
    wrapper.cache_info = lambda: {"size": len(cache), "hits": 0, "misses": 0}

    return wrapper


def retry(max_attempts: int = 3, delay: float = 1.0, backoff: float = 2.0):
    """Decorator to retry function on failure.

    Args:
        max_attempts: Maximum number of attempts
        delay: Initial delay between attempts
        backoff: Multiplier for delay after each failure

    Returns:
        Decorator function
    """

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            import time

            current_delay = delay
            last_exception = None

            for attempt in range(max_attempts):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    if attempt < max_attempts - 1:
                        time.sleep(current_delay)
                        current_delay *= backoff

            raise last_exception

        return wrapper

    return decorator
