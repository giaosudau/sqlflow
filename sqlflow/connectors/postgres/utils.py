import logging
from typing import Any, Dict

logger = logging.getLogger(__name__)


def translate_postgres_parameters(config: Dict[str, Any]) -> Dict[str, Any]:
    """Translate industry-standard parameters to psycopg2-compatible names.

    Provides backward compatibility by supporting both naming conventions:
    - Industry standard: database, username (Airbyte/Fivetran compatible)
    - psycopg2 legacy: dbname, user

    Args:
        config: Original configuration parameters

    Returns:
        Translated configuration with psycopg2-compatible parameter names
    """
    translated = config.copy()

    # Parameter mapping: industry_standard -> psycopg2_name
    param_mapping = {
        "database": "dbname",
        "username": "user",
    }

    for industry_std, psycopg2_name in param_mapping.items():
        if industry_std in config and psycopg2_name not in config:
            translated[psycopg2_name] = config[industry_std]
            logger.debug(
                f"PostgresConnector: Translated parameter '{industry_std}' -> '{psycopg2_name}' "
                f"for psycopg2 compatibility"
            )

    # Also handle table parameter mapping for consistency
    if "table" in config and "table_name" not in config:
        translated["table_name"] = config["table"]
        logger.debug(
            "PostgresConnector: Mapped 'table' parameter to 'table_name'"
        )

    return translated 