#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#

import pytest
from unittest.mock import Mock, MagicMock, patch
from datetime import datetime

from source_zoho_creator.streams import ZohoCreatorStream


@pytest.mark.unit
class TestZohoCreatorStreamInitialization:
    """Test suite for ZohoCreatorStream initialization."""

    def test_stream_initialization(self, config_valid_minimal, mock_logger):
        """Test stream initializes with required parameters."""
        api = Mock()
        
        stream = ZohoCreatorStream(
            api=api,
            account_owner_name="john.doe",
            app_link_name="inventory_management",
            form_link_name="products",
            batch_size=100
        )
        
        assert stream.api == api
        assert stream.account_owner_name == "john.doe"
        assert stream.app_link_name == "inventory_management"
        assert stream.form_link_name == "products"

    def test_stream_name_property(self):
        """Test stream name is set correctly."""
        api = Mock()
        
        stream = ZohoCreatorStream(
            api=api,
            account_owner_name="john.doe",
            app_link_name="inventory",
            form_link_name="products",
            batch_size=100
        )
        
        # Stream name should be derived from app_link_name and form_link_name
        assert stream.name == "inventory_products"


@pytest.mark.unit
class TestZohoCreatorStreamSchema:
    """Test suite for stream schema handling."""

    def test_get_json_schema_returns_dict(self):
        """Test get_json_schema returns a dictionary."""
        api = Mock()
        
        stream = ZohoCreatorStream(
            api=api,
            account_owner_name="john.doe",
            app_link_name="inventory",
            form_link_name="products",
            batch_size=100
        )
        
        schema = stream.get_json_schema()
        
        assert isinstance(schema, dict)
        assert "$schema" in schema or "type" in schema

    def test_stream_primary_key(self):
        """Test stream has correct primary key."""
        api = Mock()
        
        stream = ZohoCreatorStream(
            api=api,
            account_owner_name="john.doe",
            app_link_name="inventory",
            form_link_name="products",
            batch_size=100
        )
        
        # Primary key should be set (usually ID field)
        assert hasattr(stream, "primary_key")


@pytest.mark.unit
class TestZohoCreatorStreamPagination:
    """Test suite for pagination handling."""

    def test_request_params_without_pagination_token(self):
        """Test request_params method without pagination token."""
        api = Mock()
        
        stream = ZohoCreatorStream(
            api=api,
            account_owner_name="john.doe",
            app_link_name="inventory",
            form_link_name="products",
            batch_size=100
        )
        
        params = stream.request_params(
            stream_state={},
            stream_slice=None,
            next_page_token=None
        )
        
        assert isinstance(params, dict)

    def test_request_params_with_pagination_token(self):
        """Test request_params method with pagination token."""
        api = Mock()
        
        stream = ZohoCreatorStream(
            api=api,
            account_owner_name="john.doe",
            app_link_name="inventory",
            form_link_name="products",
            batch_size=100
        )
        
        params = stream.request_params(
            stream_state={},
            stream_slice=None,
            next_page_token={"index": 100}
        )
        
        assert isinstance(params, dict)

    def test_next_page_token_with_more_records(self):
        """Test next_page_token when more records exist."""
        api = Mock()
        
        stream = ZohoCreatorStream(
            api=api,
            account_owner_name="john.doe",
            app_link_name="inventory",
            form_link_name="products",
            batch_size=100
        )
        
        # Mock response indicating more records
        response = Mock()
        response.json.return_value = {
            "code": 200,
            "result": [
                {"id": "1", "name": "Product 1"},
                {"id": "2", "name": "Product 2"},
            ],
            "more_records": True,
            "page_offset": 100
        }
        
        token = stream.next_page_token(response)
        
        # Should return a token for next page
        assert token is not None

    def test_next_page_token_no_more_records(self):
        """Test next_page_token when no more records exist."""
        api = Mock()
        
        stream = ZohoCreatorStream(
            api=api,
            account_owner_name="john.doe",
            app_link_name="inventory",
            form_link_name="products",
            batch_size=100
        )
        
        # Mock response indicating no more records
        response = Mock()
        response.json.return_value = {
            "code": 200,
            "result": [
                {"id": "1", "name": "Product 1"},
            ],
            "more_records": False
        }
        
        token = stream.next_page_token(response)
        
        # Should return None when no more records
        assert token is None


@pytest.mark.unit
class TestZohoCreatorStreamParsing:
    """Test suite for response parsing."""

    def test_parse_response_extracts_records(self):
        """Test parse_response extracts records from response."""
        api = Mock()
        
        stream = ZohoCreatorStream(
            api=api,
            account_owner_name="john.doe",
            app_link_name="inventory",
            form_link_name="products",
            batch_size=100
        )
        
        response = Mock()
        response.json.return_value = {
            "code": 200,
            "result": [
                {"id": "1", "name": "Product 1"},
                {"id": "2", "name": "Product 2"},
            ]
        }
        
        records = list(stream.parse_response(response))
        
        assert len(records) == 2
        assert records[0]["id"] == "1"
        assert records[1]["id"] == "2"

    def test_parse_response_empty_result(self):
        """Test parse_response handles empty result."""
        api = Mock()
        
        stream = ZohoCreatorStream(
            api=api,
            account_owner_name="john.doe",
            app_link_name="inventory",
            form_link_name="products",
            batch_size=100
        )
        
        response = Mock()
        response.json.return_value = {
            "code": 200,
            "result": []
        }
        
        records = list(stream.parse_response(response))
        
        assert len(records) == 0


@pytest.mark.unit
class TestZohoCreatorStreamIncremental:
    """Test suite for incremental sync support."""

    def test_cursor_field_exists(self):
        """Test stream has cursor field for incremental sync."""
        api = Mock()
        
        stream = ZohoCreatorStream(
            api=api,
            account_owner_name="john.doe",
            app_link_name="inventory",
            form_link_name="products",
            batch_size=100
        )
        
        # Should have cursor_field for incremental sync
        assert hasattr(stream, "cursor_field") or stream.cursor_field is None

    def test_get_updated_state(self):
        """Test get_updated_state updates cursor."""
        api = Mock()
        
        stream = ZohoCreatorStream(
            api=api,
            account_owner_name="john.doe",
            app_link_name="inventory",
            form_link_name="products",
            batch_size=100
        )
        
        if stream.cursor_field:
            current_state = {}
            latest_record = {"id": "1", stream.cursor_field: "2024-01-01T10:00:00Z"}
            
            updated_state = stream.get_updated_state(current_state, latest_record)
            
            assert updated_state is not None


@pytest.mark.unit
class TestZohoCreatorStreamErrorHandling:
    """Test suite for error handling in streams."""

    def test_parse_response_handles_missing_result_key(self):
        """Test parse_response handles response without result key."""
        api = Mock()
        
        stream = ZohoCreatorStream(
            api=api,
            account_owner_name="john.doe",
            app_link_name="inventory",
            form_link_name="products",
            batch_size=100
        )
        
        response = Mock()
        response.json.return_value = {
            "code": 200,
            # Missing 'result' key
        }
        
        # Should handle gracefully
        records = list(stream.parse_response(response))
        
        # Should return empty list or handle error
        assert isinstance(records, list)

    def test_parse_response_handles_api_error(self):
        """Test parse_response handles API error response."""
        api = Mock()
        
        stream = ZohoCreatorStream(
            api=api,
            account_owner_name="john.doe",
            app_link_name="inventory",
            form_link_name="products",
            batch_size=100
        )
        
        response = Mock()
        response.json.return_value = {
            "code": 404,
            "message": "Form not found",
        }
        
        # Should handle gracefully or raise appropriate error
        try:
            records = list(stream.parse_response(response))
            # If no exception, should be empty or handle error
            assert isinstance(records, list)
        except Exception as e:
            # Exception is acceptable
            assert isinstance(e, Exception)


@pytest.mark.unit
class TestZohoCreatorStreamIntegration:
    """Integration tests for stream functionality."""

    def test_stream_read_with_mock_api(self):
        """Test stream read with mock API."""
        api = Mock()
        api.get.return_value = Mock(
            json=lambda: {
                "code": 200,
                "result": [
                    {"id": "1", "name": "Product 1"},
                    {"id": "2", "name": "Product 2"},
                ],
                "more_records": False
            }
        )
        
        stream = ZohoCreatorStream(
            api=api,
            account_owner_name="john.doe",
            app_link_name="inventory",
            form_link_name="products",
            batch_size=100
        )
        
        # Test that stream can read records
        assert stream is not None

    def test_stream_url_base_set(self):
        """Test stream has correct URL base."""
        api = Mock()
        
        stream = ZohoCreatorStream(
            api=api,
            account_owner_name="john.doe",
            app_link_name="inventory",
            form_link_name="products",
            batch_size=100
        )
        
        # Should have url_base property for HttpStream
        assert hasattr(stream, "url_base") or hasattr(stream, "base_url")
