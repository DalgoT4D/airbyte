#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#

import logging
import pytest
from unittest.mock import Mock, patch, MagicMock

from source_zoho_creator import SourceZohoCreator


@pytest.mark.unit
class TestSourceZohoCreatorCheckConnection:
    """Test suite for check_connection method."""

    def test_check_connection_success(self, config_valid_minimal, mock_logger, mocker):
        """Test successful connection check with valid config."""
        source = SourceZohoCreator()
        
        mock_api = mocker.patch("source_zoho_creator.source.ZohoCreatorAPI")
        mock_api.return_value.validate_config.return_value = (True, None)
        
        success, error = source.check_connection(mock_logger, config_valid_minimal)
        
        assert success is True
        assert error is None

    def test_check_connection_invalid_credentials(self, config_valid_minimal, mock_logger, mocker):
        """Test connection check fails with invalid credentials."""
        source = SourceZohoCreator()
        
        mock_api = mocker.patch("source_zoho_creator.source.ZohoCreatorAPI")
        mock_api.return_value.validate_config.return_value = (
            False,
            "Authentication failed. Invalid refresh token."
        )
        
        success, error = source.check_connection(mock_logger, config_valid_minimal)
        
        assert success is False
        assert "Authentication" in error

    def test_check_connection_missing_required_field(self, config_invalid_missing_field, mock_logger):
        """Test connection check fails when required field is missing."""
        source = SourceZohoCreator()
        
        success, error = source.check_connection(mock_logger, config_invalid_missing_field)
        
        assert success is False
        assert error is not None

    def test_check_connection_empty_account_owner(self, config_invalid_empty_username, mock_logger):
        """Test connection check fails with empty account owner name."""
        source = SourceZohoCreator()
        
        success, error = source.check_connection(mock_logger, config_invalid_empty_username)
        
        assert success is False
        assert error is not None

    def test_check_connection_empty_app_link(self, config_invalid_empty_app_link, mock_logger):
        """Test connection check fails with empty app link name."""
        source = SourceZohoCreator()
        
        success, error = source.check_connection(mock_logger, config_invalid_empty_app_link)
        
        assert success is False
        assert error is not None

    def test_check_connection_api_not_found(self, config_valid_minimal, mock_logger, mocker):
        """Test connection check fails when app/account not found."""
        source = SourceZohoCreator()
        
        mock_api = mocker.patch("source_zoho_creator.source.ZohoCreatorAPI")
        mock_api.return_value.validate_config.return_value = (
            False,
            "Application or account not found."
        )
        
        success, error = source.check_connection(mock_logger, config_valid_minimal)
        
        assert success is False
        assert "not found" in error.lower()

    def test_check_connection_generic_error(self, config_valid_minimal, mock_logger, mocker):
        """Test connection check handles generic exceptions."""
        source = SourceZohoCreator()
        
        mock_api = mocker.patch("source_zoho_creator.source.ZohoCreatorAPI")
        mock_api.side_effect = Exception("Unexpected error")
        
        success, error = source.check_connection(mock_logger, config_valid_minimal)
        
        assert success is False
        assert error is not None

    def test_check_connection_eu_datacenter(self, config_eu_datacenter, mock_logger, mocker):
        """Test connection check with EU datacenter config."""
        source = SourceZohoCreator()

        mock_api = mocker.patch("source_zoho_creator.source.ZohoCreatorAPI")
        mock_api.return_value.validate_config.return_value = (True, None)

        success, error = source.check_connection(mock_logger, config_eu_datacenter)

        # Verify API was initialized with EU datacenter
        mock_api.assert_called_once()
        call_kwargs = mock_api.call_args[1]
        assert call_kwargs["datacenter"] == "EU"
        assert success is True


@pytest.mark.unit
class TestSourceZohoCreatorStreams:
    """Test suite for streams method."""

    def test_streams_returns_list(self, config_valid_minimal, mocker):
        """Test streams method returns a list."""
        source = SourceZohoCreator()
        
        mock_api = mocker.patch("source_zoho_creator.source.ZohoCreatorAPI")
        mock_api.return_value.list_applications.return_value = []
        mock_api.return_value.get_application_forms.return_value = []
        
        streams = source.streams(config_valid_minimal)
        
        assert isinstance(streams, list)

    def test_streams_with_no_applications(self, config_valid_minimal, mocker):
        """Test streams when no applications are available."""
        source = SourceZohoCreator()
        
        mock_api = mocker.patch("source_zoho_creator.source.ZohoCreatorAPI")
        mock_api.return_value.list_applications.return_value = []
        
        streams = source.streams(config_valid_minimal)
        
        assert streams == []

    def test_streams_with_reports(self, config_valid_minimal, mocker):
        """Test streams method creates one stream instance per discovered report."""
        source = SourceZohoCreator()

        mock_api = mocker.patch("source_zoho_creator.source.ZohoCreatorAPI")
        mock_api_instance = mock_api.return_value
        mock_api_instance.get_application_reports.return_value = [
            {"link_name": "All_Products"},
            {"link_name": "All_Stock"},
        ]
        mock_api_instance.get_authenticator.return_value = MagicMock()

        streams = source.streams(config_valid_minimal)

        # Should create 2 stream instances (one per report)
        assert len(streams) == 2

    def test_streams_specific_app_link(self, config_valid_minimal, mocker):
        """Test streams calls get_application_reports for the configured app."""
        source = SourceZohoCreator()

        config = config_valid_minimal.copy()
        config["app_link_name"] = "specific_app"

        mock_api = mocker.patch("source_zoho_creator.source.ZohoCreatorAPI")
        mock_api_instance = mock_api.return_value
        mock_api_instance.get_application_reports.return_value = [
            {"link_name": "report_1"}
        ]
        mock_api_instance.get_authenticator.return_value = MagicMock()

        source.streams(config)

        mock_api_instance.get_application_reports.assert_called_once()


@pytest.mark.unit
class TestSourceZohoCreatorDiscover:
    """Test suite for discover method."""

    def test_discover_returns_catalog(self, config_valid_minimal, mock_logger, mocker):
        """Test discover method returns a catalog."""
        source = SourceZohoCreator()
        
        mock_api = mocker.patch("source_zoho_creator.source.ZohoCreatorAPI")
        mock_api.return_value.list_applications.return_value = []
        
        catalog = source.discover(mock_logger, config_valid_minimal)

        from airbyte_cdk.models import AirbyteCatalog
        assert isinstance(catalog, AirbyteCatalog)


@pytest.mark.unit
class TestSourceZohoCreatorRead:
    """Test suite for read method."""

    def test_read_yields_messages(self, config_valid_minimal, mock_logger, mocker):
        """Test read method yields AirbyteMessage instances."""
        source = SourceZohoCreator()
        
        mock_api = mocker.patch("source_zoho_creator.source.ZohoCreatorAPI")
        mock_api.return_value.list_applications.return_value = []
        
        # Mock catalog and state
        catalog = mocker.MagicMock()
        state = mocker.MagicMock()
        
        messages = list(source.read(mock_logger, config_valid_minimal, catalog, state))
        
        # Should return an iterable (could be empty if no streams)
        assert isinstance(messages, list)


@pytest.mark.unit
class TestSourceZohoCreatorIntegration:
    """Integration-like tests for full workflow."""

    def test_full_workflow_valid_config(self, config_valid_minimal, mock_logger, mocker):
        """Test complete workflow: check -> discover -> streams."""
        source = SourceZohoCreator()
        
        mock_api = mocker.patch("source_zoho_creator.source.ZohoCreatorAPI")
        mock_api_instance = mock_api.return_value
        mock_api_instance.validate_config.return_value = (True, None)
        mock_api_instance.list_applications.return_value = []
        
        # Check connection
        success, error = source.check_connection(mock_logger, config_valid_minimal)
        assert success is True
        
        # Discover
        catalog = source.discover(mock_logger, config_valid_minimal)
        assert catalog is not None
        
        # Get streams
        streams = source.streams(config_valid_minimal)
        assert isinstance(streams, list)

    def test_config_with_all_required_fields(self, config_valid_full, mock_logger, mocker):
        """Test all required config fields are passed to API."""
        source = SourceZohoCreator()

        mock_api = mocker.patch("source_zoho_creator.source.ZohoCreatorAPI")
        mock_api.return_value.validate_config.return_value = (True, None)

        source.check_connection(mock_logger, config_valid_full)

        # Verify all config fields were passed
        mock_api.assert_called_once()
        call_kwargs = mock_api.call_args[1]

        assert "client_id" in call_kwargs
        assert "client_secret" in call_kwargs
        assert "client_refresh_token" in call_kwargs
        assert "account_owner_name" in call_kwargs
        assert "app_link_name" in call_kwargs
        assert "datacenter" in call_kwargs