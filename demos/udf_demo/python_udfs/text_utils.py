"""Text utility UDFs for SQLFlow demo."""

import re

from sqlflow.udfs import python_scalar_udf


@python_scalar_udf
def capitalize_words(text: str) -> str:
    """Capitalize each word in a string.

    Args:
        text: Input string

    Returns:
        String with each word capitalized
    """
    if text is None:
        return None
    return " ".join(word.capitalize() for word in text.split())


@python_scalar_udf
def extract_domain(email: str) -> str:
    """Extract domain from an email address.

    Args:
        email: Email address

    Returns:
        Domain part of the email
    """
    if email is None or "@" not in email:
        return None
    return email.split("@")[1]


@python_scalar_udf
def count_words(text: str) -> int:
    """Count the number of words in a text.

    Args:
        text: Input text

    Returns:
        Number of words
    """
    if text is None:
        return 0
    return len(text.split())


@python_scalar_udf
def is_valid_email(email: str) -> bool:
    """Validate if a string is a properly formatted email address.

    Args:
        email: Email string to validate

    Returns:
        True if email is valid, False otherwise
    """
    if email is None:
        return False

    # Simple regex for email validation
    pattern = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
    return bool(re.match(pattern, email))
