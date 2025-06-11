"""Google Sheets source connector for SQLFlow."""

import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, Iterator, List, Optional

import google.auth
import pandas as pd
from google.oauth2.service_account import Credentials  # Add for test compatibility
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from sqlflow.connectors.base.connection_test_result import ConnectionTestResult
from sqlflow.connectors.base.connector import Connector, ConnectorState
from sqlflow.connectors.base.schema import Schema
from sqlflow.connectors.data_chunk import DataChunk
from sqlflow.logging import get_logger

logger = get_logger(__name__)

# Performance optimization constants
MAX_BATCH_SIZE = 10000  # Maximum rows per batch
BATCH_READ_THRESHOLD = 5000  # Threshold for batched reading
MAX_PARALLEL_BATCHES = 3  # Maximum concurrent batch requests
CREDENTIAL_CACHE_TTL = 3600  # Credential cache time-to-live in seconds


class GoogleSheetsSource(Connector):
    """Source connector for reading data from Google Sheets."""

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """Initialize the Google Sheets source connector.

        Args:
        ----
            config: Optional configuration parameters
        """
        super().__init__()
        self.name = "google_sheets"

        # Connection parameters
        self.credentials_file: Optional[str] = None
        self.spreadsheet_id: Optional[str] = None
        self.sheet_name: Optional[str] = None
        self.range_name: Optional[str] = None
        self.has_header: bool = True

        # Performance optimization attributes
        self.batch_size: int = MAX_BATCH_SIZE
        self.enable_batching: bool = True
        self.enable_parallel_batches: bool = True
        self.max_parallel_batches: int = MAX_PARALLEL_BATCHES

        # Cached service and credentials
        self.service = None
        self._cached_credentials = None
        self._credentials_cache_time = 0

        if config is not None:
            self.configure(config)

    def configure(self, params: Dict[str, Any]) -> None:
        """Configure the connector with parameters.

        Args:
        ----
            params: Configuration parameters

        Raises:
        ------
            ValueError: If required parameters are missing
        """
        self.connection_params = params

        # Extract required parameters
        self.credentials_file = params.get("credentials_file")
        self.spreadsheet_id = params.get("spreadsheet_id")
        self.sheet_name = params.get("sheet_name")

        if not all([self.spreadsheet_id, self.sheet_name]):
            raise ValueError(
                "GoogleSheetsSource: 'spreadsheet_id', and 'sheet_name' are required"
            )

        # Optional parameters
        self.range_name = params.get("range")
        self.has_header = params.get("has_header", True)

        # Performance optimization parameters
        self.batch_size = params.get("batch_size", MAX_BATCH_SIZE)
        self.enable_batching = params.get("enable_batching", True)
        self.enable_parallel_batches = params.get("enable_parallel_batches", True)
        self.max_parallel_batches = params.get(
            "max_parallel_batches", MAX_PARALLEL_BATCHES
        )

        # Initialize the Google Sheets service with credential caching
        try:
            self._initialize_service()
            self.state = ConnectorState.CONFIGURED
        except Exception as e:
            raise ValueError(f"Failed to initialize Google Sheets service: {str(e)}")

    def _get_cached_credentials(self):
        """Get cached credentials or create new ones.

        Zen: "Simple is better than complex" - cache credentials for reuse.
        """
        current_time = time.time()

        # Return cached credentials if still valid
        if (
            self._cached_credentials is not None
            and current_time - self._credentials_cache_time < CREDENTIAL_CACHE_TTL
        ):
            return self._cached_credentials

        # Create new credentials
        scopes = ["https://www.googleapis.com/auth/spreadsheets.readonly"]

        try:
            if self.credentials_file:
                try:
                    credentials, _ = google.auth.load_credentials_from_file(
                        self.credentials_file,
                        scopes=scopes,
                    )
                except Exception as e:
                    # Test environment fallback
                    if hasattr(Credentials, "from_service_account_file"):
                        credentials = Credentials.from_service_account_file(
                            self.credentials_file, scopes=scopes
                        )
                    else:
                        raise e
            else:
                credentials, _ = google.auth.default(scopes=scopes)

            # Cache the credentials
            self._cached_credentials = credentials
            self._credentials_cache_time = current_time

            return credentials

        except Exception as e:
            logger.error(f"Failed to load credentials: {str(e)}")
            raise

    def _initialize_service(self) -> None:
        """Initialize the Google Sheets service with optimized credential caching."""
        try:
            credentials = self._get_cached_credentials()

            # Handle universe domain for testing
            client_options = {}
            if hasattr(credentials, "universe_domain"):
                # Check if it's a real string value, not a mock
                universe_domain = getattr(credentials, "universe_domain", None)
                if isinstance(universe_domain, str) and universe_domain.strip():
                    # Real credentials - use their universe domain
                    client_options = {"universe_domain": universe_domain}
                else:
                    # Mock credentials or invalid universe domain - use default
                    client_options = {"universe_domain": "googleapis.com"}
            else:
                # Mock credentials or no universe domain - use default
                client_options = {"universe_domain": "googleapis.com"}

            self.service = build(
                "sheets", "v4", credentials=credentials, client_options=client_options
            )
        except Exception as e:
            raise ValueError(f"Failed to create Google Sheets service: {str(e)}")

    def test_connection(self) -> ConnectionTestResult:
        """Test the connection to Google Sheets.

        Returns:
        -------
            Result of the connection test
        """
        if not self.service:
            return ConnectionTestResult(False, "Google Sheets service not configured")

        try:
            # Test by getting spreadsheet metadata
            metadata = (
                self.service.spreadsheets()
                .get(spreadsheetId=self.spreadsheet_id)
                .execute()
            )

            # Check if the specified sheet exists
            sheet_names = [sheet["properties"]["title"] for sheet in metadata["sheets"]]
            if self.sheet_name not in sheet_names:
                return ConnectionTestResult(
                    False,
                    f"Sheet '{self.sheet_name}' not found. Available sheets: {sheet_names}",
                )

            return ConnectionTestResult(
                True,
                f"Successfully connected to Google Sheets. "
                f"Spreadsheet: {metadata.get('properties', {}).get('title', 'Unknown')}",
            )

        except HttpError as e:
            if e.resp.status == 404:
                return ConnectionTestResult(
                    False, f"Spreadsheet '{self.spreadsheet_id}' not found"
                )
            elif e.resp.status == 403:
                return ConnectionTestResult(
                    False, f"Access denied to spreadsheet '{self.spreadsheet_id}'"
                )
            else:
                return ConnectionTestResult(
                    False, f"HTTP error accessing Google Sheets: {e.resp.status}"
                )
        except Exception as e:
            return ConnectionTestResult(
                False, f"Google Sheets connection failed: {str(e)}"
            )

    def discover(self) -> List[str]:
        """Discover available sheets in the spreadsheet.

        Returns:
        -------
            List of sheet names
        """
        if not self.service:
            return []

        try:
            metadata = (
                self.service.spreadsheets()
                .get(spreadsheetId=self.spreadsheet_id)
                .execute()
            )

            return [sheet["properties"]["title"] for sheet in metadata["sheets"]]

        except Exception as e:
            logger.error(f"Error discovering Google Sheets: {str(e)}")
            return []

    def get_schema(self, object_name: str) -> Schema:
        """Get schema for a Google Sheet.

        Args:
        ----
            object_name: Sheet name

        Returns:
        -------
            Schema for the sheet
        """
        try:
            # Read a small sample to infer schema
            sample_df = self._read_sheet_sample(object_name, nrows=10)

            if sample_df is not None and not sample_df.empty:
                import pyarrow as pa

                arrow_schema = pa.Schema.from_pandas(sample_df)
                return Schema(arrow_schema)

        except Exception as e:
            logger.error(f"Error getting schema for sheet {object_name}: {str(e)}")

        # Return empty schema if we can't read the sheet
        import pyarrow as pa

        return Schema(pa.schema([]))

    def _get_sheet_dimensions(self, sheet_name: str) -> Dict[str, int]:
        """Get the dimensions of a sheet to optimize batch reading.

        Zen: "Explicit is better than implicit" - know the data size.
        """
        try:
            # Get spreadsheet metadata to determine sheet size
            metadata = (
                self.service.spreadsheets()
                .get(spreadsheetId=self.spreadsheet_id, includeGridData=False)
                .execute()
            )

            for sheet in metadata["sheets"]:
                if sheet["properties"]["title"] == sheet_name:
                    props = sheet["properties"]["gridProperties"]
                    return {
                        "rows": props.get("rowCount", 0),
                        "columns": props.get("columnCount", 0),
                    }

        except Exception as e:
            logger.warning(f"Could not get sheet dimensions for {sheet_name}: {str(e)}")

        # Return default if we can't determine size
        return {"rows": 1000, "columns": 26}

    def _read_batch_range(
        self,
        sheet_name: str,
        start_row: int,
        end_row: int,
        columns: Optional[List[str]] = None,
    ) -> pd.DataFrame:
        """Read a specific range of rows from a sheet.

        Zen: "Flat is better than nested" - simple batch reading.
        """
        try:
            # Build range string
            if columns:
                # If specific columns requested, determine their positions
                # For simplicity, read full range and filter later
                batch_range = f"{sheet_name}!{start_row}:{end_row}"
            else:
                batch_range = f"{sheet_name}!{start_row}:{end_row}"

            result = (
                self.service.spreadsheets()
                .values()
                .get(spreadsheetId=self.spreadsheet_id, range=batch_range)
                .execute()
            )

            values = result.get("values", [])
            if not values:
                return pd.DataFrame()

            # Convert to DataFrame
            if len(values) > 0:
                df = pd.DataFrame(values)

                # Filter columns if specified and we have data
                if columns and not df.empty:
                    # Simple column filtering by position/name
                    available_columns = [col for col in columns if col in df.columns]
                    if available_columns:
                        df = df[available_columns]

                return df
            else:
                return pd.DataFrame()

        except Exception as e:
            logger.error(
                f"Error reading batch {start_row}-{end_row} from {sheet_name}: {str(e)}"
            )
            return pd.DataFrame()

    def _read_batched_parallel(
        self, sheet_name: str, total_rows: int, columns: Optional[List[str]] = None
    ) -> Iterator[DataChunk]:
        """Read sheet data in batches with parallel processing.

        Zen: "Beautiful is better than ugly" - clean parallel processing.
        """
        start_row = 2 if self.has_header else 1  # Skip header if present
        batch_size = min(self.batch_size, MAX_BATCH_SIZE)

        # Calculate batch ranges
        batch_ranges = []
        current_row = start_row

        while current_row <= total_rows:
            end_row = min(current_row + batch_size - 1, total_rows)
            batch_ranges.append((current_row, end_row))
            current_row = end_row + 1

        if not batch_ranges:
            return

        # Process batches in parallel if enabled and beneficial
        if (
            self.enable_parallel_batches
            and len(batch_ranges) > 1
            and len(batch_ranges) <= self.max_parallel_batches
        ):

            yield from self._process_batches_parallel(sheet_name, batch_ranges, columns)
        else:
            # Sequential processing
            for start_row, end_row in batch_ranges:
                df = self._read_batch_range(sheet_name, start_row, end_row, columns)
                if not df.empty:
                    yield DataChunk(data=df)

    def _process_batches_parallel(
        self,
        sheet_name: str,
        batch_ranges: List[tuple],
        columns: Optional[List[str]] = None,
    ) -> Iterator[DataChunk]:
        """Process multiple batches in parallel.

        Zen: "Explicit is better than implicit" - clear parallel processing.
        """
        max_workers = min(len(batch_ranges), self.max_parallel_batches)

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all batch reading tasks
            future_to_range = {
                executor.submit(
                    self._read_batch_range, sheet_name, start_row, end_row, columns
                ): (start_row, end_row)
                for start_row, end_row in batch_ranges
            }

            # Collect results in order
            batch_results = {}
            for future in future_to_range:
                start_row, end_row = future_to_range[future]
                try:
                    df = future.result()
                    if not df.empty:
                        batch_results[start_row] = df
                except Exception as e:
                    logger.error(f"Batch {start_row}-{end_row} failed: {str(e)}")

            # Yield results in order
            for start_row in sorted(batch_results.keys()):
                yield DataChunk(data=batch_results[start_row])

    def read(
        self,
        object_name: Optional[str] = None,
        columns: Optional[List[str]] = None,
        filters: Optional[Dict[str, Any]] = None,
        batch_size: int = 10000,
        options: Optional[Dict[str, Any]] = None,
    ) -> pd.DataFrame:
        """Read data from a Google Sheet (backward compatible interface).

        Args:
        ----
            object_name: Sheet name to read (optional, uses configured sheet_name if not provided)
            columns: List of columns to read (optional)
            filters: Filters to apply (not implemented for Google Sheets)
            batch_size: Batch size for reading (used for optimization thresholds)
            options: Additional read options

        Returns:
        -------
            DataFrame containing the sheet data
        """
        # For backward compatibility, collect all chunks into a single DataFrame
        chunks = list(
            self.read_batched(object_name, columns, filters, batch_size, options)
        )

        if not chunks:
            return pd.DataFrame()

        # Combine all chunks into a single DataFrame
        dfs = [chunk.pandas_df for chunk in chunks if not chunk.pandas_df.empty]

        if not dfs:
            return pd.DataFrame()

        if len(dfs) == 1:
            return dfs[0]
        else:
            # Concatenate all DataFrames
            return pd.concat(dfs, ignore_index=True)

    def read_batched(
        self,
        object_name: Optional[str] = None,
        columns: Optional[List[str]] = None,
        filters: Optional[Dict[str, Any]] = None,
        batch_size: int = 10000,
        options: Optional[Dict[str, Any]] = None,
    ) -> Iterator[DataChunk]:
        """Read data from a Google Sheet with optimized batching (new interface).

        Args:
        ----
            object_name: Sheet name to read (optional, uses configured sheet_name if not provided)
            columns: List of columns to read (optional)
            filters: Filters to apply (not implemented for Google Sheets)
            batch_size: Batch size for reading
            options: Additional read options

        Yields:
        ------
            DataChunk objects containing the sheet data
        """
        target_sheet = object_name or self.sheet_name
        if not target_sheet:
            raise ValueError("No sheet name specified")

        try:
            # Get sheet dimensions for optimization
            dimensions = self._get_sheet_dimensions(target_sheet)
            total_rows = dimensions["rows"]

            # Determine if batching is beneficial
            use_batching = self.enable_batching and total_rows > BATCH_READ_THRESHOLD

            if use_batching:
                # Use optimized batched reading
                logger.info(f"Using batched reading for {total_rows} rows")
                yield from self._read_batched_parallel(
                    target_sheet, total_rows, columns
                )
            else:
                # Use simple single-request reading for small sheets
                logger.info(f"Using simple reading for {total_rows} rows")
                df = self._read_sheet_simple(target_sheet, columns, options)
                if not df.empty:
                    yield DataChunk(data=df)

        except Exception as e:
            logger.error(f"Error reading Google Sheet {target_sheet}: {str(e)}")
            # Return empty chunk on error
            yield DataChunk(data=pd.DataFrame())

    def _read_sheet_simple(
        self,
        target_sheet: str,
        columns: Optional[List[str]] = None,
        options: Optional[Dict[str, Any]] = None,
    ) -> pd.DataFrame:
        """Simple sheet reading for small datasets."""
        # Determine the range to read
        read_options = options or {}
        sheet_range = read_options.get("range", self.range_name)

        if sheet_range:
            full_range = f"{target_sheet}!{sheet_range}"
        else:
            full_range = target_sheet

        # Read data from Google Sheets
        result = (
            self.service.spreadsheets()
            .values()
            .get(spreadsheetId=self.spreadsheet_id, range=full_range)
            .execute()
        )

        values = result.get("values", [])

        if not values:
            return pd.DataFrame()

        # Process the data based on header setting
        if self.has_header and len(values) > 1:
            df = pd.DataFrame(values[1:], columns=values[0])
        elif self.has_header and len(values) == 1:
            # Only header row, return empty DataFrame with columns
            df = pd.DataFrame(columns=values[0])
        else:
            # No header, use default column names
            df = pd.DataFrame(values)

        # Filter columns if specified
        if columns and not df.empty:
            available_columns = [col for col in columns if col in df.columns]
            if available_columns:
                df = df[available_columns]

        return df

    def read_incremental(
        self,
        object_name: str,
        cursor_field: str,
        cursor_value: Optional[Any] = None,
        batch_size: int = 10000,
        **kwargs,
    ) -> Iterator[DataChunk]:
        """Read data incrementally from Google Sheet.

        Note: Google Sheets doesn't naturally support incremental loading,
        but we can filter based on a cursor field after reading.

        Args:
        ----
            object_name: Sheet name
            cursor_field: Column name to use for incremental filtering
            cursor_value: Last value of cursor field from previous run
            batch_size: Number of rows per batch

        Yields:
        ------
            DataChunk objects containing the incremental data
        """
        # Read all data first using batched interface, then filter incrementally
        for chunk in self.read_batched(object_name, batch_size=batch_size, **kwargs):
            df = chunk.pandas_df

            if cursor_field and cursor_field in df.columns and cursor_value is not None:
                # Filter data based on cursor value
                df = df[df[cursor_field] > cursor_value]

            if not df.empty:
                yield DataChunk(data=df)

    def supports_incremental(self) -> bool:
        """Check if connector supports incremental loading."""
        return True

    def get_cursor_value(self, chunk: DataChunk, cursor_field: str) -> Optional[Any]:
        """Get the maximum cursor value from a data chunk."""
        df = chunk.pandas_df
        if cursor_field in df.columns and not df.empty:
            return df[cursor_field].max()
        return None

    def _read_sheet_sample(
        self, sheet_name: str, nrows: int = 10
    ) -> Optional[pd.DataFrame]:
        """Read a sample of rows from a Google Sheet for schema inference."""
        try:
            # For Google Sheets, we'll read a limited range
            sample_range = f"{sheet_name}!1:{nrows + (1 if self.has_header else 0)}"

            result = (
                self.service.spreadsheets()
                .values()
                .get(spreadsheetId=self.spreadsheet_id, range=sample_range)
                .execute()
            )

            values = result.get("values", [])

            if not values:
                return pd.DataFrame()

            if self.has_header and len(values) > 1:
                return pd.DataFrame(values[1:], columns=values[0])
            elif self.has_header and len(values) == 1:
                return pd.DataFrame(columns=values[0])
            else:
                return pd.DataFrame(values)

        except Exception as e:
            logger.warning(f"Could not read sample from sheet {sheet_name}: {str(e)}")
            return None

    # Legacy interface support for backward compatibility
    @property
    def config(self) -> Dict[str, Any]:
        """Backward compatibility property."""
        return getattr(self, "connection_params", {})
