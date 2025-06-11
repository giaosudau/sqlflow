from sqlflow.connectors.registry.destination_registry import (
    destination_registry,
)
from sqlflow.connectors.registry.enhanced_registry import enhanced_registry
from sqlflow.connectors.registry.source_registry import source_registry
from sqlflow.connectors.s3.destination import S3Destination
from sqlflow.connectors.s3.source import S3Source

# Register with old registries for backward compatibility
source_registry.register("s3", S3Source)
destination_registry.register("s3", S3Destination)

# Register with enhanced registry for new profile integration
enhanced_registry.register_source(
    "s3",
    S3Source,
    default_config={"region": "us-east-1", "file_format": "csv"},
    required_params=["bucket"],
    optional_params={
        "key": "",
        "path_prefix": "",
        "region": "us-east-1",
        "file_format": "csv",
        "access_key_id": "",
        "secret_access_key": "",
        "endpoint_url": "",
    },
    description="S3 object storage source connector",
)
enhanced_registry.register_destination(
    "s3",
    S3Destination,
    required_params=["uri"],
    description="S3 object storage destination connector",
)
