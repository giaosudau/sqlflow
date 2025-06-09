from sqlflow.connectors.registry.destination_registry import (
    destination_registry,
)
from sqlflow.connectors.registry.source_registry import source_registry
from sqlflow.connectors.s3.destination import S3Destination
from sqlflow.connectors.s3.source import S3Source

source_registry.register("s3", S3Source)
destination_registry.register("s3", S3Destination)
