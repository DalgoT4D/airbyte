#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#

import json
import pytest
from datetime import datetime, timedelta
from unittest.mock import Mock, MagicMock, patch
from requests.models import Response


# ============================================================================
# Configuration Fixtures
# ============================================================================

@pytest.fixture
def config_valid_minimal():
    """Minimal valid configuration (required fields only)."""
    return {
        "client_id": "1000.test_client_id",
        "client_secret": "test_client_secret",
        "client_refresh_token": "1000.test_refresh_token",
        "account_owner_name": "john.doe",
        "app_link_name": "inventory_management",
        "base_accounts_url": "accounts.zoho.com",
        "base_url": "www.zohoapis.com",
    }


@pytest.fixture
def config_valid_full():
    """Full configuration with all fields (same as minimal since spec has no optional fields)."""
    return {
        "client_id": "1000.test_client_id",
        "client_secret": "test_client_secret",
        "client_refresh_token": "1000.test_refresh_token",
        "account_owner_name": "john.doe",
        "app_link_name": "inventory_management",
        "base_accounts_url": "accounts.zoho.com",
        "base_url": "www.zohoapis.com",
    }


@pytest.fixture
def config_eu_datacenter():
    """Configuration for EU data center."""
    return {
        "client_id": "1000.test_client_id_eu",
        "client_secret": "test_client_secret_eu",
        "client_refresh_token": "1000.test_refresh_token_eu",
        "account_owner_name": "jane.smith",
        "app_link_name": "crm_system",
        "base_accounts_url": "accounts.zoho.eu",
        "base_url": "www.zohoapis.eu",
    }


@pytest.fixture
def config_au_datacenter():
    """Configuration for Australia data center."""
    return {
        "client_id": "1000.test_client_id_au",
        "client_secret": "test_client_secret_au",
        "client_refresh_token": "1000.test_refresh_token_au",
        "account_owner_name": "alice.johnson",
        "app_link_name": "erp_system",
        "base_accounts_url": "accounts.zoho.com.au",
        "base_url": "www.zohoapis.com.au",
    }


@pytest.fixture
def config_invalid_missing_field():
    """Invalid config - missing required field (client_refresh_token)."""
    return {
        "client_id": "1000.test_client_id",
        "client_secret": "test_client_secret",
        "account_owner_name": "john.doe",
        "app_link_name": "inventory_management",
        "base_accounts_url": "accounts.zoho.com",
        "base_url": "www.zohoapis.com",
    }


@pytest.fixture
def config_invalid_empty_username():
    """Invalid config - empty account owner name."""
    return {
        "client_id": "1000.test_client_id",
        "client_secret": "test_client_secret",
        "client_refresh_token": "1000.test_refresh_token",
        "account_owner_name": "",
        "app_link_name": "inventory_management",
        "base_accounts_url": "accounts.zoho.com",
        "base_url": "www.zohoapis.com",
    }


@pytest.fixture
def config_invalid_empty_app_link():
    """Invalid config - empty app link name."""
    return {
        "client_id": "1000.test_client_id",
        "client_secret": "test_client_secret",
        "client_refresh_token": "1000.test_refresh_token",
        "account_owner_name": "john.doe",
        "app_link_name": "",
        "base_accounts_url": "accounts.zoho.com",
        "base_url": "www.zohoapis.com",
    }


# ============================================================================
# Mock Response Fixtures
# ============================================================================

@pytest.fixture
def mock_token_response():
    """Mock OAuth token refresh response."""
    response = Mock(spec=Response)
    response.status_code = 200
    response.json.return_value = {
        "access_token": "test_access_token_123",
        "expires_in": 3600,  # 1 hour
        "token_type": "Bearer",
    }
    return response


@pytest.fixture
def mock_token_response_string_expires():
    """Mock OAuth token refresh response with expires_in as string."""
    response = Mock(spec=Response)
    response.status_code = 200
    response.json.return_value = {
        "access_token": "test_access_token_456",
        "expires_in": "3600",  # String instead of int
        "token_type": "Bearer",
    }
    return response


@pytest.fixture
def mock_auth_error_response():
    """Mock authentication error response."""
    response = Mock(spec=Response)
    response.status_code = 401
    response.json.return_value = {
        "error": "invalid_grant",
        "error_description": "Invalid refresh token",
    }
    response.text = json.dumps(response.json.return_value)
    return response


@pytest.fixture
def mock_not_found_response():
    """Mock 404 not found response."""
    response = Mock(spec=Response)
    response.status_code = 404
    response.json.return_value = {
        "error": "invalid_request",
        "error_description": "Application not found",
    }
    response.text = json.dumps(response.json.return_value)
    return response


@pytest.fixture
def mock_metadata_api_response():
    """Mock metadata API response for form schema."""
    response = Mock(spec=Response)
    response.status_code = 200
    response.json.return_value = {
        "code": 200,
        "message": "The form has been retrieved successfully.",
        "result": {
            "form_id": "form_123",
            "form_name": "Inventory Form",
            "form_link_name": "inventory_form",
            "fields": [
                {
                    "field_id": "field_1",
                    "field_name": "Product Name",
                    "data_type": "text",
                },
                {
                    "field_id": "field_2",
                    "field_name": "Quantity",
                    "data_type": "number",
                },
            ],
        },
    }
    return response


# ============================================================================
# API Client Fixtures
# ============================================================================

@pytest.fixture
def mock_api(config_valid_minimal):
    """Fixture providing a mock API client with methods."""
    with patch("source_zoho_creator.source.ZohoCreatorAPI") as mock_api_class:
        mock_instance = MagicMock()
        mock_instance.get_access_token.return_value = "mock_access_token_123"
        mock_instance.get_authenticator.return_value = Mock()
        mock_instance.validate_config.return_value = (True, None)
        mock_instance.list_applications.return_value = []
        mock_instance.get_application_forms.return_value = []
        
        mock_api_class.return_value = mock_instance
        yield mock_api_class


@pytest.fixture
def mock_api_with_apps(mock_api):
    """Mock API with application data."""
    mock_instance = mock_api.return_value
    mock_instance.list_applications.return_value = [
        {"app_id": "app_1", "app_name": "Inventory Management", "app_link_name": "inventory_management"},
        {"app_id": "app_2", "app_name": "CRM System", "app_link_name": "crm_system"},
    ]
    return mock_api


@pytest.fixture
def mock_api_with_forms(mock_api_with_apps):
    """Mock API with form data."""
    mock_instance = mock_api_with_apps.return_value
    mock_instance.get_application_forms.return_value = [
        {
            "form_id": "form_1",
            "form_name": "Inventory Form",
            "form_link_name": "inventory_form",
        },
        {
            "form_id": "form_2",
            "form_name": "Stock Adjustment",
            "form_link_name": "stock_adjustment",
        },
    ]
    return mock_api_with_apps


# ============================================================================
# Logger Fixtures
# ============================================================================

@pytest.fixture
def mock_logger():
    """Fixture providing a mock logger."""
    return Mock()


# ============================================================================
# Pytest Configuration
# ============================================================================

def pytest_configure(config):
    """Configure pytest with custom markers."""
    config.addinivalue_line(
        "markers", "unit: mark test as a unit test"
    )
    config.addinivalue_line(
        "markers", "integration: mark test as an integration test"
    )
    config.addinivalue_line(
        "markers", "slow: mark test as slow (deselect with '-m \"not slow\"')"
    )
