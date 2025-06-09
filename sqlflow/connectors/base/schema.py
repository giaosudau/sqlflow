from typing import Dict

import pyarrow as pa


class Schema:
    """Schema information for a data source or destination."""

    def __init__(self, arrow_schema: pa.Schema):
        """Initialize a Schema.

        Args:
        ----
            arrow_schema: Arrow schema

        """
        self.arrow_schema = arrow_schema

    @classmethod
    def from_dict(cls, schema_dict: Dict[str, str]) -> "Schema":
        """Create a Schema from a dictionary.

        Args:
        ----
            schema_dict: Dictionary mapping field names to types

        Returns:
        -------
            Schema instance

        """
        fields = []
        for name, type_str in schema_dict.items():
            if type_str.lower() == "string":
                pa_type = pa.string()
            elif type_str.lower() == "int" or type_str.lower() == "integer":
                pa_type = pa.int64()
            elif type_str.lower() == "float":
                pa_type = pa.float64()
            elif type_str.lower() == "bool" or type_str.lower() == "boolean":
                pa_type = pa.bool_()
            elif type_str.lower() == "date":
                pa_type = pa.date32()
            elif type_str.lower() == "timestamp":
                pa_type = pa.timestamp("ns")
            else:
                pa_type = pa.string()  # Default to string for unknown types

            fields.append(pa.field(name, pa_type))

        return cls(pa.schema(fields))
