#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#

import pytest
import json
import time
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime, timedelta

from source_zoho_creator.api import ZohoCreatorAPI, ZohoCreatorAPIError, ZohoCreatorAuthError


@pytest.mark.unit
class TestZohoCreatorAPIInitialization:
    """Test suite for ZohoCreatorAPI initialization."""

    def test_api_initialization_with_all_fields(self, config_valid_minimal):
        """Test API client initializes correctly with all required fields."""
        api = ZohoCreatorAPI(**config_valid_minimal)
        
        assert api.client_id == config_valid_minimal["client_id"]
        assert api.client_secret == config_valid_minimal["client_secret"]
        assert api.client_refresh_token == config_valid_minimal["client_refresh_token"]
        assert api.account_owner_name == config_valid_minimal["account_owner_name"]
        assert api.app_link_name == config_valid_minimal["app_link_name"]
        assert api.base_accounts_url == config_valid_minimal["base_accounts_url"]
        assert api.base_url == config_valid_minimal["base_url"]

    def test_api_initialization_eu_datacenter(self, config_eu_datacenter):
        """Test API initializes with EU datacenter endpoints."""
        api = ZohoCreatorAPI(**config_eu_datacenter)
        
        assert api.base_accounts_url == "accounts.zoho.eu"
        assert api.base_url == "www.zohoapis.eu"

    def test_api_initialization_au_datacenter(self, config_au_datacenter):
        """Test API initializes with AU datacenter endpoints."""
        api = ZohoCreatorAPI(**config_au_datacenter)
        
        assert api.base_accounts_url == "accounts.zoho.com.au"
        assert api.base_url == "www.zohoapis.com.au"


@pytest.mark.unit
class TestZohoCreatorAPITokenRefresh:
    """Test suite for token refresh functionality."""

    @patch("source_zoho_creator.api.requests.post")
    def test_get_access_token_success(self, mock_post, config_valid_minimal, mock_token_response):
        """Test successful token refresh."""
        mock_post.return_value = mock_token_response
        
        api = ZohoCreatorAPI(**config_valid_minimal)
        token = api.get_access_token()
        
        assert token == "test_access_token_123"
        assert api._access_token == "test_access_token_123"

    @patch("source_zoho_creator.api.requests.post")
    def test_get_access_token_expires_in_as_string(self, mock_post, config_valid_minimal, mock_token_response_string_expires):
        """Test token refresh when expires_in is returned as string."""
        mock_post.return_value = mock_token_response_string_expires
        
        api = ZohoCreatorAPI(**config_valid_minimal)
        token = api.get_access_token()
        
        assert token == "test_access_token_456"

    @patch("source_zoho_creator.api.requests.post")
    def test_get_access_token_caching(self, mock_post, config_valid_minimal, mock_token_response):
        """Test that token is cached and not refreshed on subsequent calls."""
        mock_post.return_value = mock_token_response
        
        api = ZohoCreatorAPI(**config_valid_minimal)
        token1 = api.get_access_token()
        token2 = api.get_access_token()
        
        # Token should be cached, not refreshed
        assert token1 == token2
        assert mock_post.call_count == 1  # Should only be called once

    @patch("source_zoho_creator.api.requests.post")
    def test_get_access_token_refresh_when_expired(self, mock_post, config_valid_minimal, mock_token_response):
        """Test token is refreshed when expiry is within buffer (60 seconds)."""
        mock_post.return_value = mock_token_response
        
        api = ZohoCreatorAPI(**config_valid_minimal)
        token1 = api.get_access_token()
        
        # Manually set expiry to less than buffer time
        api._token_expires_at = time.time() + 30  # 30 seconds (< 60 second buffer)
        token2 = api.get_access_token()
        
        # Should refresh since within buffer
        assert mock_post.call_count == 2

    @patch("source_zoho_creator.api.requests.post")
    def test_get_access_token_auth_error(self, mock_post, config_valid_minimal, mock_auth_error_response):
        """Test handling of authentication error during token refresh."""
        mock_post.return_value = mock_auth_error_response
        
        api = ZohoCreatorAPI(**config_valid_minimal)
        
        with pytest.raises(ZohoCreatorAuthError):
            api.get_access_token()


@pytest.mark.unit
class TestZohoCreatorAPIValidation:
    """Test suite for configuration validation."""

    @patch("source_zoho_creator.api.requests.post")
    @patch("source_zoho_creator.api.requests.get")
    def test_validate_config_success(self, mock_get, mock_post, config_valid_minimal, mock_token_response, mock_metadata_api_response):
        """Test successful config validation."""
        mock_post.return_value = mock_token_response
        mock_get.return_value = mock_metadata_api_response
        
        api = ZohoCreatorAPI(**config_valid_minimal)
        success, error = api.validate_config()
        
        assert success is True
        assert error is None

    @patch("source_zoho_creator.api.requests.post")
    def test_validate_config_token_refresh_fails(self, mock_post, config_valid_minimal, mock_auth_error_response):
        """Test config validation fails when token refresh fails."""
        mock_post.return_value = mock_auth_error_response
        
        api = ZohoCreatorAPI(**config_valid_minimal)
        success, error = api.validate_config()
        
        assert success is False
        assert "Authentication failed" in error

    @patch("source_zoho_creator.api.requests.post")
    @patch("source_zoho_creator.api.requests.get")
    def test_validate_config_api_call_fails(self, mock_get, mock_post, config_valid_minimal, mock_token_response, mock_not_found_response):
        """Test config validation fails when API call returns 404."""
        mock_post.return_value = mock_token_response
        mock_get.return_value = mock_not_found_response
        
        api = ZohoCreatorAPI(**config_valid_minimal)
        success, error = api.validate_config()
        
        assert success is False
        assert "not found" in error.lower()


@pytest.mark.unit
class TestZohoCreatorAPIAuthenticator:
    """Test suite for authenticator generation."""

    @patch("source_zoho_creator.api.requests.post")
    def test_get_authenticator_returns_token_authenticator(self, mock_post, config_valid_minimal, mock_token_response):
        """Test get_authenticator returns properly configured TokenAuthenticator."""
        mock_post.return_value = mock_token_response
        
        api = ZohoCreatorAPI(**config_valid_minimal)
        authenticator = api.get_authenticator()
        
        # Should return TokenAuthenticator instance
        from airbyte_cdk.sources.streams.http.requests_native_auth import TokenAuthenticator
        assert isinstance(authenticator, TokenAuthenticator)


@pytest.mark.unit
class TestZohoCreatorAPIErrorHandling:
    """Test suite for error handling."""

    def test_zoho_creator_api_error_exception(self):
        """Test ZohoCreatorAPIError exception can be raised."""
        with pytest.raises(ZohoCreatorAPIError):
            raise ZohoCreatorAPIError("Test error")

    def test_zoho_creator_auth_error_exception(self):
        """Test ZohoCreatorAuthError is subclass of ZohoCreatorAPIError."""
        error = ZohoCreatorAuthError("Auth failed")
        assert isinstance(error, ZohoCreatorAPIError)

    @patch("source_zoho_creator.api.requests.post")
    def test_validate_config_handles_generic_exception(self, mock_post, config_valid_minimal):
        """Test config validation handles generic exceptions gracefully."""
        mock_post.side_effect = Exception("Network error")
        
        api = ZohoCreatorAPI(**config_valid_minimal)
        success, error = api.validate_config()
        
        assert success is False
        assert error is not None
