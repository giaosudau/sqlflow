"""Text utility UDFs for SQLFlow ecommerce example."""

import logging

from sqlflow.udfs import python_scalar_udf

# Set up logging
logger = logging.getLogger(__name__)


@python_scalar_udf
def format_name(name: str) -> str:
    """Format customer name with proper capitalization.

    Args:
        name: Customer name

    Returns:
        Properly formatted name
    """
    if name is None:
        return None

    # Split by spaces and capitalize each part
    parts = name.strip().split()
    formatted_parts = [part.capitalize() for part in parts]
    return " ".join(formatted_parts)


@python_scalar_udf
def mask_email(email: str, gdpr_consent: str) -> str:
    """Mask email according to GDPR consent status.

    Args:
        email: Email address
        gdpr_consent: GDPR consent status ("true" or "false")

    Returns:
        Original or masked email depending on consent
    """
    if email is None:
        return None

    # Return original email if consent given (case insensitive check)
    if gdpr_consent and gdpr_consent.lower() == "true":
        return email

    # Otherwise mask email
    username, domain = email.split("@")
    if len(username) <= 2:
        masked_username = "*" * len(username)
    else:
        masked_username = username[0] + "*" * (len(username) - 2) + username[-1]

    return f"{masked_username}@{domain}"


@python_scalar_udf
def extract_domain(email: str) -> str:
    """Extract domain from email address.

    Args:
        email: Email address

    Returns:
        Domain part of email
    """
    if email is None or "@" not in email:
        return None

    return email.split("@")[1]


@python_scalar_udf
def categorize_domain(domain: str) -> str:
    """Categorize email domain by type.

    Args:
        domain: Email domain

    Returns:
        Domain category
    """
    if domain is None:
        return "Unknown"

    domain = domain.lower()

    # Common free email providers
    free_providers = ["gmail.com", "yahoo.com", "hotmail.com", "outlook.com", "aol.com"]

    # Business domains
    business_domains = [
        "example.com"
    ]  # In real data, this would include actual business domains

    # Education domains
    edu_domains = [".edu"]

    if domain in free_providers:
        return "Personal"
    elif any(domain.endswith(edu) for edu in edu_domains):
        return "Education"
    elif domain in business_domains or "." in domain.split(".")[0]:
        return "Business"
    else:
        return "Other"
