#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#

from abc import abstractmethod
from typing import Any, Iterable, Mapping, MutableMapping, Optional

from airbyte_cdk.sources.streams.http import HttpStream

from .api import ZohoCreatorAPI

class ZohoCreatorStream(HttpStream):
    """Base class for Zoho Creator streams backed by the Creator v2.1 API."""

    def __init__(self, api: ZohoCreatorAPI):
        super().__init__(authenticator=api.get_authenticator())
        self.api = api

    @property
    def name(self) -> str:
        return f"{self.app_link_name}"

    @property
    @abstractmethod
    def path(self) -> str:
        """Return the path for the specific Creator resource."""
        return ""

    @property
    @abstractmethod
    def primary_key(self) -> Optional[str]:
        return None

    @property
    def http_method(self) -> str:
        return "GET"

    @property
    @abstractmethod
    def cursor_field(self) -> str:
        return ""

    def request_headers(
        self,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, Any],
        next_page_token: Optional[Any],
    ) -> Mapping[str, Any]:
        headers = dict(
            super().request_headers(stream_state=stream_state, stream_slice=stream_slice, next_page_token=next_page_token)
        )
        if next_page_token:
            headers["record_cursor"] = next_page_token
        return headers

    def next_page_token(self, response) -> Optional[str]:
        return response.headers.get("record_cursor")

    def get_json_schema(self) -> Mapping[str, Any]:
        return {}

    


class ReportDataStream(ZohoCreatorStream):
    """Stream for Zoho Creator form/report data using API v2.1."""

    def __init__(self, api: ZohoCreatorAPI, report_link_name: str):
        super().__init__(api)
        self.report_link_name = report_link_name

    @property
    def request_url(self) -> str:
        return f"https://{self.api.base_url}/creator/v2.1/data/{self.api.account_owner_name}/{self.api.app_link_name}/report/{self.report_link_name}"

    @property
    def path(self) -> str:
        return f"/creator/v2.1/data/{self.api.account_owner_name}/{self.api.app_link_name}/report/{self.report_link_name}"

    @property
    def primary_key(self) -> str:
        return "ID"

    @property
    def cursor_field(self) -> str:
        return "Modified_Time"

    def request_params(
        self,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, Any],
        next_page_token: Optional[Any],
    ) -> MutableMapping[str, Any]:
        params: MutableMapping[str, Any] = {
            "max_records": 1000,
            "field_config": "all",
        }

        if stream_state and self.cursor_field in stream_state:
            last_modified = stream_state[self.cursor_field]
            params["criteria"] = f'Modified_Time > "{last_modified}"'

        return params

    def parse_response(self, response) -> Iterable[Mapping[str, Any]]:
        data = response.json()
        if data.get("code") == 3000:
            yield from data.get("data", [])
            return

        error_msg = data.get("message", "Unknown API error")
        raise Exception(f"Zoho API error: {error_msg}")

    def get_updated_state(
        self,
        current_stream_state: Mapping[str, Any],
        latest_record: Mapping[str, Any],
    ) -> Mapping[str, Any]:
        if self.cursor_field in latest_record:
            latest_cursor = latest_record[self.cursor_field]
            return {self.cursor_field: latest_cursor}

        return current_stream_state

    def get_json_schema(self) -> Mapping[str, Any]:
        return {
            "type": "object",
            "properties": {
                "ID": {"type": "string"},
                "Modified_Time": {"type": "string", "format": "date-time"},
                "Added_Time": {"type": "string", "format": "date-time"},
                "Added_User": {"type": "string"},
            },
        }

    
