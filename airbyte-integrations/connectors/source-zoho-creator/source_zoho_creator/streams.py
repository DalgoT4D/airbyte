#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#

import datetime
import logging
from typing import Any, Iterable, Mapping, MutableMapping, Optional

from airbyte_cdk.models import ConfiguredAirbyteStream, SyncMode
from airbyte_cdk.sources.streams.core import StreamData
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.utils.slice_logger import SliceLogger
from airbyte_cdk.sources.streams.concurrent.cursor import ConnectorStateManager
from airbyte_cdk.sources.utils.schema_helpers import InternalConfig

# try:
#     from airbyte_cdk.sources.streams.http.config import HttpConfig as InternalConfig
# except Exception:
#     InternalConfig = Any


from .api import ZohoCreatorAPI
from .exceptions import ZohoCreatorAPIError, ZohoCreatorAuthError

logger = logging.getLogger("airbyte")


class ReportDataStream(HttpStream):
    """Stream for Zoho Creator form/report data using API v2.1."""

    # Date format for Added_Time field: "04-Oct-2023 12:35:24"
    DATE_FORMAT = "%d-%b-%Y %H:%M:%S"
    PRIMARY_KEY = "ID"
    DEFAULT_CURSOR_FIELDS = ["Added_Time", "Modified_Time"]
    MAX_RECORDS = 1000

    def __init__(self, api: ZohoCreatorAPI, report_link_name: str):
        """Initialize stream for a specific Zoho Creator report."""
        # Set instance attributes before super().__init__() to ensure they're available
        # if HttpStream initialization accesses properties like 'name'
        self.api = api
        self.report_link_name = report_link_name
        self._configured_cursor_field: Optional[str] = None
        self._sync_mode: Optional[SyncMode] = None
        super().__init__(authenticator=api.get_authenticator())

    @property
    def url_base(self) -> str:
        """Base URL for Zoho Creator Data API v2.1 report endpoints."""
        # Ensure url_base ends with / so urljoin appends path correctly
        return f"https://{self.api.base_url}/creator/v2.1/data/{self.api.account_owner_name}/{self.api.app_link_name}/report/"

    @property
    def name(self) -> str:
        """Stream name is the report link name."""
        return self.report_link_name

    def path(
        self,
        *,
        stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Optional[Any] = None,
    ) -> str:
        """API endpoint path for this report (relative to url_base).
        
        Returns path without leading slash to work with urljoin.
        urljoin treats paths starting with / as absolute, which replaces the base path.
        """
        return self.report_link_name

    @property
    def primary_key(self) -> str:
        """Primary key field is 'ID' for all Zoho Creator records."""
        return self.PRIMARY_KEY

    @property
    def http_method(self) -> str:
        """HTTP method for API requests (GET for reading data)."""
        return "GET"

    @property
    def raise_on_http_errors(self) -> bool:
        """Disable CDK's automatic raise_for_status().

        Zoho returns HTTP 404 (not 200) for code 3100 "no records found".
        If we let the CDK raise on 4xx, parse_response is never reached and
        every incremental sync with no new records fails. We handle all HTTP
        error cases manually in parse_response instead.
        """
        return False

    @property
    def cursor_field(self) -> str:
        """
        Return cursor field if 'Added_Time', 'Modified_Time' exists in schema, otherwise None.
        
        Returns None to disable incremental sync (full refresh only).
        """

        schema = self.api.get_report_schema(self.report_link_name)

        found_cursor_fields = []

        for cursor_field in self.DEFAULT_CURSOR_FIELDS:
            if cursor_field in schema:
                found_cursor_fields.append(cursor_field)
        
        if not found_cursor_fields:
            return []
        else:
            return found_cursor_fields

    def _state_cursor_field(self) -> Optional[str]:
        """
        Return the single cursor field to use for state tracking.

        Returns None immediately for full-refresh syncs — no state is tracked and
        there is no need to fetch the schema to check for cursor field presence.
        If a configured catalog provided a specific cursor field, prefer that.
        Otherwise, fall back to the first available default cursor field.
        """
        if self._sync_mode == SyncMode.full_refresh:
            return None
        if self._configured_cursor_field:
            return self._configured_cursor_field
        cf = self.cursor_field
        return cf[0] if isinstance(cf, list) and cf else None

    def read(
        self,
        configured_stream: ConfiguredAirbyteStream,
        logger: logging.Logger,
        slice_logger: SliceLogger,
        stream_state: MutableMapping[str, Any],
        state_manager: ConnectorStateManager,
        internal_config: InternalConfig
    ) -> Iterable[StreamData]:
        """
        Override to capture the sync mode and cursor_field from the configured catalog.
        """
        self._sync_mode = configured_stream.sync_mode

        if configured_stream.cursor_field:
            # Only set _configured_cursor_field if cursor_field is present and non-empty in the configured catalog.
            if configured_stream.cursor_field and len(configured_stream.cursor_field) > 0:
                self._configured_cursor_field = configured_stream.cursor_field[0]
            else:
                self._configured_cursor_field = None

        return super().read(
            configured_stream,
            logger,
            slice_logger,
            stream_state,
            state_manager,
            internal_config
        )

    def request_headers(
        self,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, Any],
        next_page_token: Optional[Any],
    ) -> Mapping[str, Any]:
        """Build HTTP headers for requests, including pagination cursor if present."""
        headers = dict(
            super().request_headers(
                stream_state=stream_state, stream_slice=stream_slice, next_page_token=next_page_token
            )
        )
        if next_page_token:
            token_value = next_page_token.get("record_cursor") if isinstance(next_page_token, dict) else next_page_token
            headers["record_cursor"] = token_value
        return headers

    def next_page_token(self, response) -> Optional[str]:
        """Extract pagination token from response headers (Zoho uses 'record_cursor' header)."""
        token = response.headers.get("record_cursor")
        if token:
            return {"record_cursor": token}
        return None

    def request_params(
        self,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, Any],
        next_page_token: Optional[Any],
    ) -> MutableMapping[str, Any]:
        """Build query parameters: set max records and add incremental filter if state exists."""
        params: MutableMapping[str, Any] = {"max_records": self.MAX_RECORDS}

        state_cursor_field = self._state_cursor_field()

        if state_cursor_field and stream_state and state_cursor_field in stream_state:
            last_cursor_time = stream_state[state_cursor_field]
            params["criteria"] = f'{state_cursor_field} > "{last_cursor_time}"'

        logger.info("Params: %s", params)
        return params

    # Zoho Creator API codes that indicate "no records" — not a failure.
    # See: https://www.zoho.com/creator/help/api/v2/status-codes.html
    NO_RECORDS_CODES = {
        3100,  # No records found for the given criteria (incremental sync with no new data)
        3930,  # No reports available
        3920,  # No pages available
        3910,  # No forms available
    }

    def parse_response(self, response, **kwargs) -> Iterable[Mapping[str, Any]]:
        """Parse API response, handling both success and all error cases.

        Zoho returns HTTP 404 + code 3100 when no records match the criteria filter
        (e.g. incremental sync with no new data). Because raise_on_http_errors is
        False, non-200 responses reach here and must be handled explicitly.
        """
        try:
            data = response.json()
        except Exception:
            raise ZohoCreatorAPIError(
                f"Non-JSON response (HTTP {response.status_code}): {response.text[:200]}"
            )

        code = data.get("code")

        # Empty-result codes: treat as a valid empty page, not a failure.
        # Notably, code 3100 ("no records for criteria") arrives as HTTP 404.
        if code in self.NO_RECORDS_CODES:
            logger.info("Zoho API returned code %s (%s) — treating as empty result set.", code, data.get("message", ""))
            return

        # For non-200 responses that aren't a known empty-result code, raise now.
        if response.status_code != 200:
            error_msg = data.get("message", response.text[:200])
            msg_lower = error_msg.lower()
            if any(kw in msg_lower for kw in ("invalid token", "authentication", "unauthorized", "access denied")):
                raise ZohoCreatorAuthError(f"Authentication error (code {code}): {error_msg}")
            raise ZohoCreatorAPIError(f"HTTP {response.status_code}, Zoho API error (code {code}): {error_msg}")

        if code == 3000:
            yield from data.get("data", [])
            return

        error_msg = data.get("message", "Unknown API error")

        # Distinguish auth failures so operators know to rotate credentials rather
        # than treating it as a transient API error.
        msg_lower = error_msg.lower()
        if any(kw in msg_lower for kw in ("invalid token", "authentication", "unauthorized", "access denied")):
            raise ZohoCreatorAuthError(f"Authentication error (code {code}): {error_msg}")

        raise ZohoCreatorAPIError(f"Zoho API error (code {code}): {error_msg}")

    def get_updated_state(
        self,
        current_stream_state: Mapping[str, Any],
        latest_record: Mapping[str, Any],
    ) -> Mapping[str, Any]:
        """Update stream state with latest cursor value from the most recent record.
        
        Parses date strings in format '04-Oct-2023 12:35:24' and ensures we always
        track the maximum cursor value seen, even if records arrive out of order.
        """
        try:
            state_cursor_field = self._state_cursor_field()

            if not state_cursor_field:
                return current_stream_state or {}

            if state_cursor_field not in latest_record:
                return current_stream_state or {}
        except TypeError as e:
            raise

        try:
            # Parse the latest record's cursor value
            state_cursor_field = self._state_cursor_field()
            latest_cursor_str = latest_record[state_cursor_field]
            latest_cursor_dt = datetime.datetime.strptime(latest_cursor_str, self.DATE_FORMAT)

            # Get current state value if it exists
            current_cursor_str = (current_stream_state or {}).get(state_cursor_field)
            if current_cursor_str:
                current_cursor_dt = datetime.datetime.strptime(current_cursor_str, self.DATE_FORMAT)
                # Return the maximum (most recent) date
                max_cursor_dt = max(latest_cursor_dt, current_cursor_dt)
            else:
                max_cursor_dt = latest_cursor_dt

            # Return as string in original format
            updated_state = {state_cursor_field: max_cursor_dt.strftime(self.DATE_FORMAT)}
            logger.info(f"Updated state: {updated_state}")
            return updated_state

        except (ValueError, TypeError) as e:
            # If parsing fails, log and return current state
            logger.warning(f"Failed to parse {self.cursor_field} value: {e}. Using current state.")
            return current_stream_state or {}

    def get_json_schema(self) -> Mapping[str, Any]:
        """Generate JSON schema dynamically by fetching sample data and inferring field types."""
        return {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": self.api.get_report_schema(self.report_link_name),
            "additionalProperties": True,
        }