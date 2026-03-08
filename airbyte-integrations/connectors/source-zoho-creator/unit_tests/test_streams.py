#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#

import pytest
from unittest.mock import Mock, MagicMock, patch
from datetime import datetime

from source_zoho_creator.streams import ReportDataStream
from source_zoho_creator.exceptions import ZohoCreatorAPIError


def _make_stream(report_link_name="All_Products", base_url="www.zohoapis.com"):
    """Create a ReportDataStream with a mock API."""
    api = Mock()
    api.base_url = base_url
    api.account_owner_name = "john.doe"
    api.app_link_name = "inventory_management"
    api.get_authenticator.return_value = Mock()
    api.get_report_schema.return_value = {
        "ID": {"type": "string"},
        "Added_Time": {"type": "string"},
        "Modified_Time": {"type": "string"},
    }
    return ReportDataStream(api=api, report_link_name=report_link_name)


@pytest.mark.unit
class TestZohoCreatorStreamInitialization:
    """Test suite for ReportDataStream initialization."""

    def test_stream_initialization(self):
        """Test stream initializes with required parameters."""
        stream = _make_stream()

        assert stream.report_link_name == "All_Products"
        assert stream.api.account_owner_name == "john.doe"
        assert stream.api.app_link_name == "inventory_management"

    def test_stream_name_property(self):
        """Test stream name equals the report link name."""
        stream = _make_stream(report_link_name="My_Report")
        assert stream.name == "My_Report"

    def test_stream_primary_key(self):
        """Test stream has correct primary key."""
        stream = _make_stream()
        assert stream.primary_key == "ID"


@pytest.mark.unit
class TestZohoCreatorStreamSchema:
    """Test suite for schema handling."""

    def test_get_json_schema_returns_dict(self):
        """Test get_json_schema returns a well-formed JSON Schema dict."""
        stream = _make_stream()
        schema = stream.get_json_schema()

        assert isinstance(schema, dict)
        assert schema.get("type") == "object"
        assert "properties" in schema

    def test_get_json_schema_contains_inferred_fields(self):
        """Test schema properties come from the API's schema method."""
        stream = _make_stream()
        schema = stream.get_json_schema()

        assert "ID" in schema["properties"]
        assert "Added_Time" in schema["properties"]


@pytest.mark.unit
class TestZohoCreatorStreamPagination:
    """Test suite for record_cursor header-based pagination."""

    def test_next_page_token_with_cursor_header(self):
        """Test next_page_token extracts record_cursor from response headers."""
        stream = _make_stream()
        response = Mock()
        response.headers = {"record_cursor": "abc123cursor"}

        token = stream.next_page_token(response)

        assert token == {"record_cursor": "abc123cursor"}

    def test_next_page_token_no_cursor_header(self):
        """Test next_page_token returns None when no record_cursor header."""
        stream = _make_stream()
        response = Mock()
        response.headers = {}

        token = stream.next_page_token(response)

        assert token is None

    def test_request_headers_with_pagination_token(self):
        """Test request_headers includes record_cursor when paginating."""
        stream = _make_stream()
        headers = stream.request_headers(
            stream_state={},
            stream_slice=None,
            next_page_token={"record_cursor": "abc123cursor"},
        )

        assert headers.get("record_cursor") == "abc123cursor"

    def test_request_headers_without_pagination_token(self):
        """Test request_headers has no record_cursor on first page."""
        stream = _make_stream()
        headers = stream.request_headers(
            stream_state={},
            stream_slice=None,
            next_page_token=None,
        )

        assert "record_cursor" not in headers

    def test_request_params_sets_max_records(self):
        """Test request_params always includes max_records."""
        stream = _make_stream()
        params = stream.request_params(stream_state={}, stream_slice=None, next_page_token=None)

        assert "max_records" in params
        assert params["max_records"] == 1000


@pytest.mark.unit
class TestZohoCreatorStreamParsing:
    """Test suite for response parsing."""

    def test_parse_response_success_code_3000(self):
        """Test parse_response yields records when API returns code 3000."""
        stream = _make_stream()
        response = Mock()
        response.json.return_value = {
            "code": 3000,
            "data": [
                {"ID": "1", "Name": "Product A"},
                {"ID": "2", "Name": "Product B"},
            ],
        }

        records = list(stream.parse_response(response))

        assert len(records) == 2
        assert records[0]["ID"] == "1"
        assert records[1]["ID"] == "2"

    def test_parse_response_empty_data(self):
        """Test parse_response handles empty data array."""
        stream = _make_stream()
        response = Mock()
        response.json.return_value = {"code": 3000, "data": []}

        records = list(stream.parse_response(response))

        assert records == []

    def test_raise_on_http_errors_is_false(self):
        """Test raise_on_http_errors is False so 404s reach parse_response."""
        stream = _make_stream()
        assert stream.raise_on_http_errors is False

    def test_parse_response_non_3000_code_raises(self):
        """Test parse_response raises ZohoCreatorAPIError on unknown non-3000 code."""
        stream = _make_stream()
        response = Mock()
        response.status_code = 200
        response.json.return_value = {"code": 3001, "message": "Some error"}

        with pytest.raises(ZohoCreatorAPIError):
            list(stream.parse_response(response))

    def test_parse_response_http_404_code_3100_yields_empty(self):
        """Test parse_response yields no records when Zoho returns HTTP 404 + code 3100.

        Zoho returns HTTP 404 (not 200) for code 3100 "No records found for the given
        criteria". Without raise_on_http_errors=False, the CDK would raise before
        parse_response is reached, failing every incremental sync with no new data.
        """
        stream = _make_stream()
        response = Mock()
        response.status_code = 404
        response.json.return_value = {
            "code": 3100,
            "message": "No records found for the given criteria.",
        }

        records = list(stream.parse_response(response))

        assert records == []

    @pytest.mark.parametrize("no_records_code", [3930, 3920, 3910])
    def test_parse_response_other_no_records_codes_yield_empty(self, no_records_code):
        """Test parse_response yields no records for other Zoho 'no data' codes (HTTP 200)."""
        stream = _make_stream()
        response = Mock()
        response.status_code = 200
        response.json.return_value = {
            "code": no_records_code,
            "message": "No data available.",
        }

        records = list(stream.parse_response(response))

        assert records == []

    def test_parse_response_non_200_non_3100_raises(self):
        """Test parse_response raises ZohoCreatorAPIError for non-200 responses with unknown codes."""
        stream = _make_stream()
        response = Mock()
        response.status_code = 500
        response.json.return_value = {"code": 9999, "message": "Internal server error"}

        with pytest.raises(ZohoCreatorAPIError):
            list(stream.parse_response(response))


@pytest.mark.unit
class TestZohoCreatorStreamIncremental:
    """Test suite for incremental sync support."""

    def test_cursor_field_present_in_schema(self):
        """Test cursor_field returns list when Added_Time is in schema."""
        stream = _make_stream()
        cf = stream.cursor_field

        assert isinstance(cf, list)
        assert "Added_Time" in cf

    def test_cursor_field_empty_when_not_in_schema(self):
        """Test cursor_field returns empty list when no cursor fields in schema."""
        stream = _make_stream()
        stream.api.get_report_schema.return_value = {"ID": {"type": "string"}}
        stream._schema = None  # reset cache

        cf = stream.cursor_field
        assert cf == []

    def test_get_updated_state_advances_cursor(self):
        """Test get_updated_state picks the maximum cursor value."""
        stream = _make_stream()
        stream._configured_cursor_field = "Added_Time"

        current_state = {"Added_Time": "01-Jan-2024 10:00:00"}
        latest_record = {"Added_Time": "15-Jun-2024 12:00:00"}

        updated = stream.get_updated_state(current_state, latest_record)

        assert updated["Added_Time"] == "15-Jun-2024 12:00:00"

    def test_get_updated_state_keeps_max(self):
        """Test get_updated_state keeps existing state if it is more recent."""
        stream = _make_stream()
        stream._configured_cursor_field = "Added_Time"

        current_state = {"Added_Time": "15-Jun-2024 12:00:00"}
        latest_record = {"Added_Time": "01-Jan-2024 10:00:00"}

        updated = stream.get_updated_state(current_state, latest_record)

        assert updated["Added_Time"] == "15-Jun-2024 12:00:00"

    def test_request_params_adds_criteria_when_state_exists(self):
        """Test request_params adds criteria filter when stream state is set."""
        stream = _make_stream()
        stream._configured_cursor_field = "Added_Time"

        params = stream.request_params(
            stream_state={"Added_Time": "01-Jan-2024 10:00:00"},
            stream_slice=None,
            next_page_token=None,
        )

        assert "criteria" in params
        assert "Added_Time" in params["criteria"]


@pytest.mark.unit
class TestZohoCreatorStreamURLBase:
    """Test suite for URL construction."""

    def test_url_base_uses_api_base_url(self):
        """Test url_base is constructed from the api's base_url."""
        stream = _make_stream(base_url="www.zohoapis.eu")

        assert "www.zohoapis.eu" in stream.url_base

    def test_url_base_ends_with_slash(self):
        """Test url_base ends with / for correct urljoin behaviour."""
        stream = _make_stream()

        assert stream.url_base.endswith("/")

    def test_path_returns_report_link_name(self):
        """Test path() returns the report link name."""
        stream = _make_stream(report_link_name="Custom_Report")

        assert stream.path() == "Custom_Report"
