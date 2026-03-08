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
class TestZohoCreatorAPIReportData:
    """Test suite for get_report_data via SDK."""

    def test_get_report_data_returns_records(self, config_valid_minimal, mock_sdk_client):
        """Test get_report_data returns dicts from SDK Record.model_dump()."""
        record1 = Mock()
        record1.model_dump.return_value = {"ID": "1", "Name": "Product A"}
        record2 = Mock()
        record2.model_dump.return_value = {"ID": "2", "Name": "Product B"}
        mock_sdk_client.application.return_value.report.return_value.get_records.return_value = iter([record1, record2])

        api = _make_api(config_valid_minimal, mock_sdk_client)
        records = api.get_report_data("All_Products")

        assert records == [{"ID": "1", "Name": "Product A"}, {"ID": "2", "Name": "Product B"}]
        mock_sdk_client.application.assert_called_once_with("inventory_management", "john.doe")
        mock_sdk_client.application.return_value.report.assert_called_once_with("All_Products")

    def test_get_report_data_empty_report(self, config_valid_minimal, mock_sdk_client):
        """Test get_report_data returns empty list when no records."""
        mock_sdk_client.application.return_value.report.return_value.get_records.return_value = iter([])

        api = _make_api(config_valid_minimal, mock_sdk_client)
        records = api.get_report_data("Empty_Report")

        assert records == []

    def test_get_report_data_handles_sdk_exception(self, config_valid_minimal, mock_sdk_client):
        """Test get_report_data returns empty list on SDK error."""
        mock_sdk_client.application.return_value.report.return_value.get_records.side_effect = Exception("API error")

        api = _make_api(config_valid_minimal, mock_sdk_client)
        records = api.get_report_data("All_Products")

        assert records == []


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
