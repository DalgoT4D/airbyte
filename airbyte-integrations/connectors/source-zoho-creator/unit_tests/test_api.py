#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#

import pytest
from unittest.mock import Mock, patch, MagicMock, PropertyMock

from source_zoho_creator.api import ZohoCreatorAPI
from source_zoho_creator.exceptions import ZohoCreatorAPIError, ZohoCreatorAuthError


def _make_api(config, mock_sdk_client):
    """Helper: patch ZohoCreatorClient and return a ZohoCreatorAPI instance."""
    with patch("source_zoho_creator.api.ZohoCreatorClient", return_value=mock_sdk_client):
        return ZohoCreatorAPI(**config)


@pytest.fixture
def mock_sdk_client():
    """A MagicMock standing in for ZohoCreatorClient."""
    client = MagicMock()
    client.auth_handler.access_token = "test_access_token_123"
    client.auth_handler.get_auth_headers.return_value = {
        "Authorization": "Zoho-oauthtoken test_access_token_123"
    }
    return client


@pytest.mark.unit
class TestZohoCreatorAPIInitialization:
    """Test suite for ZohoCreatorAPI initialization."""

    def test_api_initialization_with_all_fields(self, config_valid_minimal, mock_sdk_client):
        """Test API client initializes correctly with all required fields."""
        api = _make_api(config_valid_minimal, mock_sdk_client)

        assert api.client_id == config_valid_minimal["client_id"]
        assert api.client_secret == config_valid_minimal["client_secret"]
        assert api.client_refresh_token == config_valid_minimal["client_refresh_token"]
        assert api.account_owner_name == config_valid_minimal["account_owner_name"]
        assert api.app_link_name == config_valid_minimal["app_link_name"]
        assert api.datacenter == "US"
        assert api.base_url == "www.zohoapis.com"

    def test_api_initialization_eu_datacenter(self, config_eu_datacenter, mock_sdk_client):
        """Test API derives correct base_url from EU datacenter."""
        api = _make_api(config_eu_datacenter, mock_sdk_client)

        assert api.datacenter == "EU"
        assert api.base_url == "www.zohoapis.eu"

    def test_api_initialization_au_datacenter(self, config_au_datacenter, mock_sdk_client):
        """Test API derives correct base_url from AU datacenter."""
        api = _make_api(config_au_datacenter, mock_sdk_client)

        assert api.datacenter == "AU"
        assert api.base_url == "www.zohoapis.com.au"


@pytest.mark.unit
class TestZohoCreatorAPITokenRefresh:
    """Test suite for token retrieval via SDK auth handler."""

    def test_get_access_token_success(self, config_valid_minimal, mock_sdk_client):
        """Test successful token retrieval via SDK."""
        api = _make_api(config_valid_minimal, mock_sdk_client)
        token = api.get_access_token()

        assert token == "test_access_token_123"
        mock_sdk_client.auth_handler.get_auth_headers.assert_called_once()

    def test_get_access_token_triggers_refresh(self, config_valid_minimal, mock_sdk_client):
        """Test that each call to get_access_token invokes SDK auth handler."""
        api = _make_api(config_valid_minimal, mock_sdk_client)
        api.get_access_token()
        api.get_access_token()

        assert mock_sdk_client.auth_handler.get_auth_headers.call_count == 2

    def test_get_access_token_returns_sdk_token(self, config_valid_minimal, mock_sdk_client):
        """Test that get_access_token returns whatever the SDK auth handler has."""
        mock_sdk_client.auth_handler.access_token = "fresh_token_xyz"
        api = _make_api(config_valid_minimal, mock_sdk_client)

        assert api.get_access_token() == "fresh_token_xyz"

    def test_get_access_token_auth_error_propagates(self, config_valid_minimal, mock_sdk_client):
        """Test that SDK auth errors surface as exceptions."""
        from zoho_creator_sdk.exceptions import AuthenticationError
        mock_sdk_client.auth_handler.get_auth_headers.side_effect = AuthenticationError("bad token")

        api = _make_api(config_valid_minimal, mock_sdk_client)

        with pytest.raises(AuthenticationError):
            api.get_access_token()


@pytest.mark.unit
class TestZohoCreatorAPIValidation:
    """Test suite for configuration validation."""

    def test_validate_config_success(self, config_valid_minimal, mock_sdk_client):
        """Test successful config validation returns True when API responds 200."""
        api = _make_api(config_valid_minimal, mock_sdk_client)

        mock_response = Mock()
        mock_response.status_code = 200

        with patch("source_zoho_creator.api.requests.get", return_value=mock_response):
            success, error = api.validate_config()

        assert success is True
        assert error is None

    def test_validate_config_auth_failure(self, config_valid_minimal, mock_sdk_client):
        """Test config validation fails on 401 response."""
        api = _make_api(config_valid_minimal, mock_sdk_client)

        mock_response = Mock()
        mock_response.status_code = 401

        with patch("source_zoho_creator.api.requests.get", return_value=mock_response):
            success, error = api.validate_config()

        assert success is False
        assert "Authentication failed" in error

    def test_validate_config_account_not_found(self, config_valid_minimal, mock_sdk_client):
        """Test config validation fails on 404 response."""
        api = _make_api(config_valid_minimal, mock_sdk_client)

        mock_response = Mock()
        mock_response.status_code = 404

        with patch("source_zoho_creator.api.requests.get", return_value=mock_response):
            success, error = api.validate_config()

        assert success is False
        assert "not found" in error.lower()

    def test_validate_config_forbidden(self, config_valid_minimal, mock_sdk_client):
        """Test config validation fails on 403 response."""
        api = _make_api(config_valid_minimal, mock_sdk_client)

        mock_response = Mock()
        mock_response.status_code = 403

        with patch("source_zoho_creator.api.requests.get", return_value=mock_response):
            success, error = api.validate_config()

        assert success is False
        assert "forbidden" in error.lower()

    def test_validate_config_handles_timeout(self, config_valid_minimal, mock_sdk_client):
        """Test config validation handles request timeout gracefully."""
        import requests as req
        api = _make_api(config_valid_minimal, mock_sdk_client)

        with patch("source_zoho_creator.api.requests.get", side_effect=req.Timeout):
            success, error = api.validate_config()

        assert success is False
        assert "timeout" in error.lower()

    def test_validate_config_handles_generic_exception(self, config_valid_minimal, mock_sdk_client):
        """Test config validation handles unexpected exceptions gracefully."""
        api = _make_api(config_valid_minimal, mock_sdk_client)

        with patch("source_zoho_creator.api.requests.get", side_effect=Exception("Network error")):
            success, error = api.validate_config()

        assert success is False
        assert error is not None


@pytest.mark.unit
class TestZohoCreatorAPIDatacenterValidation:
    """Test suite for datacenter config validation."""

    def test_invalid_datacenter_raises_config_error(self, config_valid_minimal, mock_sdk_client):
        """Test that an unsupported datacenter raises ZohoCreatorConfigError."""
        from source_zoho_creator.exceptions import ZohoCreatorConfigError
        config = {**config_valid_minimal, "datacenter": "INVALID"}
        with pytest.raises(ZohoCreatorConfigError, match="INVALID"):
            _make_api(config, mock_sdk_client)

    def test_datacenter_case_insensitive(self, mock_sdk_client):
        """Test that datacenter value is uppercased before validation."""
        config = {
            "client_id": "1000.test",
            "client_secret": "secret",
            "client_refresh_token": "token",
            "account_owner_name": "john.doe",
            "app_link_name": "app",
            "datacenter": "us",
        }
        api = _make_api(config, mock_sdk_client)
        assert api.datacenter == "US"
        assert api.base_url == "www.zohoapis.com"


@pytest.mark.unit
class TestZohoCreatorAPIReportData:
    """Test suite for get_report_data via raw HTTP requests."""

    def test_get_report_data_returns_records(self, config_valid_minimal, mock_sdk_client):
        """Test get_report_data returns records from a successful API response."""
        api = _make_api(config_valid_minimal, mock_sdk_client)

        mock_response = Mock()
        mock_response.json.return_value = {
            "code": 3000,
            "data": [
                {"ID": "1", "Name": "Product A"},
                {"ID": "2", "Name": "Product B"},
            ],
        }

        with patch("source_zoho_creator.api.requests.get", return_value=mock_response):
            records = api.get_report_data("All_Products")

        assert records == [{"ID": "1", "Name": "Product A"}, {"ID": "2", "Name": "Product B"}]

    def test_get_report_data_code_3100_returns_empty(self, config_valid_minimal, mock_sdk_client):
        """Test get_report_data returns empty list for code 3100 (no records, HTTP 404)."""
        api = _make_api(config_valid_minimal, mock_sdk_client)

        mock_response = Mock()
        mock_response.status_code = 404
        mock_response.json.return_value = {
            "code": 3100,
            "message": "No records found for the given criteria.",
        }

        with patch("source_zoho_creator.api.requests.get", return_value=mock_response):
            records = api.get_report_data("Empty_Report")

        assert records == []

    def test_get_report_data_code_9280_returns_empty(self, config_valid_minimal, mock_sdk_client):
        """Test get_report_data returns empty list for code 9280 (no records, HTTP 400)."""
        api = _make_api(config_valid_minimal, mock_sdk_client)

        mock_response = Mock()
        mock_response.status_code = 400
        mock_response.json.return_value = {
            "code": 9280,
            "message": "No records found matching the given criteria.",
        }

        with patch("source_zoho_creator.api.requests.get", return_value=mock_response):
            records = api.get_report_data("Empty_Report")

        assert records == []

    def test_get_report_data_http_error_returns_empty(self, config_valid_minimal, mock_sdk_client):
        """Test get_report_data returns empty list on unexpected HTTP error."""
        api = _make_api(config_valid_minimal, mock_sdk_client)

        mock_response = Mock()
        mock_response.status_code = 403
        mock_response.json.return_value = {"code": 1005, "message": "Permission denied."}
        mock_response.text = "Permission denied."

        with patch("source_zoho_creator.api.requests.get", return_value=mock_response):
            records = api.get_report_data("All_Products")

        assert records == []

    def test_get_report_data_handles_exception(self, config_valid_minimal, mock_sdk_client):
        """Test get_report_data returns empty list on network exception."""
        api = _make_api(config_valid_minimal, mock_sdk_client)

        with patch("source_zoho_creator.api.requests.get", side_effect=Exception("Network error")):
            records = api.get_report_data("All_Products")

        assert records == []


@pytest.mark.unit
class TestZohoCreatorAPIApplicationReports:
    """Test suite for get_application_reports."""

    def test_get_application_reports_success(self, config_valid_minimal, mock_sdk_client):
        """Test get_application_reports returns list of report dicts on success."""
        api = _make_api(config_valid_minimal, mock_sdk_client)

        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "reports": [
                {"link_name": "All_Products", "display_name": "All Products"},
                {"link_name": "All_Stock", "display_name": "All Stock"},
            ]
        }

        with patch("source_zoho_creator.api.requests.get", return_value=mock_response):
            reports = api.get_application_reports()

        assert len(reports) == 2
        assert reports[0]["link_name"] == "All_Products"

    def test_get_application_reports_http_error_returns_empty(self, config_valid_minimal, mock_sdk_client):
        """Test get_application_reports returns empty list on HTTP error."""
        api = _make_api(config_valid_minimal, mock_sdk_client)

        mock_response = Mock()
        mock_response.status_code = 403
        mock_response.text = "Forbidden"

        with patch("source_zoho_creator.api.requests.get", return_value=mock_response):
            reports = api.get_application_reports()

        assert reports == []

    def test_get_application_reports_exception_returns_empty(self, config_valid_minimal, mock_sdk_client):
        """Test get_application_reports returns empty list on network exception."""
        api = _make_api(config_valid_minimal, mock_sdk_client)

        with patch("source_zoho_creator.api.requests.get", side_effect=Exception("Network error")):
            reports = api.get_application_reports()

        assert reports == []


@pytest.mark.unit
class TestZohoCreatorAPIReportSchema:
    """Test suite for get_report_schema schema inference."""

    def test_get_report_schema_infers_string_fields(self, config_valid_minimal, mock_sdk_client):
        """Test schema inference maps string values to string type."""
        api = _make_api(config_valid_minimal, mock_sdk_client)

        mock_response = Mock()
        mock_response.json.return_value = {
            "code": 3000,
            "data": [{"ID": "1", "Name": "Product A", "Added_Time": "01-Jan-2024 10:00:00"}],
        }

        with patch("source_zoho_creator.api.requests.get", return_value=mock_response):
            schema = api.get_report_schema("All_Products")

        assert schema["ID"] == {"type": "string"}
        assert schema["Name"] == {"type": "string"}
        assert schema["Added_Time"] == {"type": "string"}

    def test_get_report_schema_infers_object_fields(self, config_valid_minimal, mock_sdk_client):
        """Test schema inference maps dict values to object type."""
        api = _make_api(config_valid_minimal, mock_sdk_client)

        mock_response = Mock()
        mock_response.json.return_value = {
            "code": 3000,
            "data": [{"ID": "1", "Address": {"City": "London", "Country": "UK"}}],
        }

        with patch("source_zoho_creator.api.requests.get", return_value=mock_response):
            schema = api.get_report_schema("All_Products")

        assert schema["Address"] == {"type": "object", "additionalProperties": True}

    def test_get_report_schema_infers_array_fields(self, config_valid_minimal, mock_sdk_client):
        """Test schema inference maps list values to array type."""
        api = _make_api(config_valid_minimal, mock_sdk_client)

        mock_response = Mock()
        mock_response.json.return_value = {
            "code": 3000,
            "data": [{"ID": "1", "Tags": ["a", "b"]}],
        }

        with patch("source_zoho_creator.api.requests.get", return_value=mock_response):
            schema = api.get_report_schema("All_Products")

        assert schema["Tags"] == {"type": "array", "items": {"type": "string"}}

    def test_get_report_schema_empty_report_returns_empty_dict(self, config_valid_minimal, mock_sdk_client):
        """Test schema inference returns empty dict for reports with no records."""
        api = _make_api(config_valid_minimal, mock_sdk_client)

        mock_response = Mock()
        mock_response.status_code = 400
        mock_response.json.return_value = {"code": 9280, "message": "No records."}

        with patch("source_zoho_creator.api.requests.get", return_value=mock_response):
            schema = api.get_report_schema("Empty_Report")

        assert schema == {}

    def test_get_report_schema_caches_result(self, config_valid_minimal, mock_sdk_client):
        """Test schema is cached — second call for same report does not hit the API."""
        api = _make_api(config_valid_minimal, mock_sdk_client)

        mock_response = Mock()
        mock_response.json.return_value = {
            "code": 3000,
            "data": [{"ID": "1"}],
        }

        with patch("source_zoho_creator.api.requests.get", return_value=mock_response) as mock_get:
            api.get_report_schema("All_Products")
            api.get_report_schema("All_Products")
            assert mock_get.call_count == 1


@pytest.mark.unit
class TestZohoCreatorAPIAuthenticator:
    """Test suite for authenticator generation."""

    def test_get_authenticator_returns_token_authenticator(self, config_valid_minimal, mock_sdk_client):
        """Test get_authenticator returns a properly configured TokenAuthenticator."""
        api = _make_api(config_valid_minimal, mock_sdk_client)
        authenticator = api.get_authenticator()

        from airbyte_cdk.sources.streams.http.requests_native_auth import TokenAuthenticator
        assert isinstance(authenticator, TokenAuthenticator)


@pytest.mark.unit
class TestZohoCreatorAPIErrorHandling:
    """Test suite for error handling."""

    def test_zoho_creator_api_error_exception(self):
        """Test ZohoCreatorAPIError exception can be raised."""
        with pytest.raises(ZohoCreatorAPIError):
            raise ZohoCreatorAPIError("Test error")

    def test_zoho_creator_auth_error_is_subclass(self):
        """Test ZohoCreatorAuthError is subclass of ZohoCreatorAPIError."""
        error = ZohoCreatorAuthError("Auth failed")
        assert isinstance(error, ZohoCreatorAPIError)
