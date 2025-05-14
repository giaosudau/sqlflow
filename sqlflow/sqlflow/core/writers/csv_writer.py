"""CSV writer for SQLFlow."""

import csv
import os
from typing import Any, Dict, Optional

import pandas as pd

from sqlflow.sqlflow.core.protocols import WriterProtocol


class CSVWriter(WriterProtocol):
    """Writes data to CSV files."""

    def write(
        self, data: Any, destination: str, options: Optional[Dict[str, Any]] = None
    ) -> None:
        """Write data to a CSV file.

        Args:
            data: Data to write (pandas DataFrame or similar)
            destination: Path to the CSV file
            options: Options for the writer
        """
        options = options or {}

        os.makedirs(os.path.dirname(os.path.abspath(destination)), exist_ok=True)

        if not isinstance(data, pd.DataFrame):
            data = pd.DataFrame(data)

        data.to_csv(
            destination,
            index=options.get("include_index", False),
            header=options.get("include_header", True),
            sep=options.get("delimiter", ","),
            quoting=options.get("quoting", csv.QUOTE_MINIMAL),
            quotechar=options.get("quotechar", '"'),
            line_terminator=options.get("line_terminator", "\n"),
            encoding=options.get("encoding", "utf-8"),
        )
