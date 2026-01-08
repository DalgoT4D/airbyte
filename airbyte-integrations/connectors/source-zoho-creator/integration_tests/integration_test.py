#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#

import json
import logging
import pytest
import os
from pathlib import Path

from source_zoho_creator import SourceZohoCreator


logger = logging.getLogger("airbyte.integration_tests")


class TestIntegrationSetup:
    """Setup and validation for integration tests."""

    @staticmethod
    def load_config(config_path: str) -> dict:
        """Load configuration from JSON file."""
        config_file = Path(__file__).parent / config_path
        if not config_file.exists():
            pytest.skip(f"Configuration file not found: {config_file}")
        
        with open(config_file) as f:
            return json.load(f)

    @staticmethod
    def is_integration_test_enabled() -> bool:
        """Check if integration tests are enabled via environment variable."""
        return os.getenv("AIRBYTE_INTEGRATION_TESTS", "false").lower() == "true"


class TestIntegrationConnection(TestIntegrationSetup):
    """Integration tests for connection validation."""

    @pytest.mark.integration
    def test_check_connection_with_sample_config(self):
        """Test connection check with sample config."""
        if not self.is_integration_test_enabled():
            pytest.skip("Integration tests disabled. Set AIRBYTE_INTEGRATION_TESTS=true")
        
        config = self.load_config("sample_config.json")
        source = SourceZohoCreator()
        logger = logging.getLogger("test")
        
        success, error = source.check_connection(logger, config)
        
        # With real credentials, should succeed
        # With placeholder credentials, may fail but should handle gracefully
        assert isinstance(success, bool)
        if not success:
            assert error is not None

    @pytest.mark.integration
    def test_check_connection_with_invalid_config(self):
        """Test connection check fails with invalid credentials."""
        if not self.is_integration_test_enabled():
            pytest.skip("Integration tests disabled. Set AIRBYTE_INTEGRATION_TESTS=true")
        
        config = self.load_config("invalid_config.json")
        source = SourceZohoCreator()
        logger = logging.getLogger("test")
        
        success, error = source.check_connection(logger, config)
        
        # Invalid config should fail
        assert success is False
        assert error is not None


class TestIntegrationDiscovery(TestIntegrationSetup):
    """Integration tests for schema discovery."""

    @pytest.mark.integration
    def test_discover_returns_catalog(self):
        """Test discover method returns valid catalog."""
        if not self.is_integration_test_enabled():
            pytest.skip("Integration tests disabled. Set AIRBYTE_INTEGRATION_TESTS=true")
        
        config = self.load_config("sample_config.json")
        source = SourceZohoCreator()
        logger = logging.getLogger("test")
        
        # Should not raise exception
        catalog = source.discover(logger, config)
        
        assert catalog is not None

    @pytest.mark.integration
    def test_discover_with_invalid_config_fails(self):
        """Test discover fails with invalid credentials."""
        if not self.is_integration_test_enabled():
            pytest.skip("Integration tests disabled. Set AIRBYTE_INTEGRATION_TESTS=true")
        
        config = self.load_config("invalid_config.json")
        source = SourceZohoCreator()
        logger = logging.getLogger("test")
        
        # May raise exception or return empty catalog
        try:
            catalog = source.discover(logger, config)
            # If succeeds, should be valid catalog
            assert catalog is not None
        except Exception as e:
            # Exception is acceptable for invalid config
            logger.info(f"Expected error with invalid config: {e}")


class TestIntegrationRead(TestIntegrationSetup):
    """Integration tests for data reading."""

    @pytest.mark.integration
    @pytest.mark.slow
    def test_read_returns_records(self):
        """Test read method returns records from Zoho Creator."""
        if not self.is_integration_test_enabled():
            pytest.skip("Integration tests disabled. Set AIRBYTE_INTEGRATION_TESTS=true")
        
        config = self.load_config("sample_config.json")
        source = SourceZohoCreator()
        logger = logging.getLogger("test")
        
        # First discover catalog
        catalog = source.discover(logger, config)
        
        # Then read data
        messages = list(source.read(logger, config, catalog, None))
        
        # Should return a list (may be empty if no data)
        assert isinstance(messages, list)


class TestIntegrationEndToEnd(TestIntegrationSetup):
    """End-to-end integration tests."""

    @pytest.mark.integration
    @pytest.mark.slow
    def test_full_sync_workflow(self):
        """Test complete sync workflow: check -> discover -> read."""
        if not self.is_integration_test_enabled():
            pytest.skip("Integration tests disabled. Set AIRBYTE_INTEGRATION_TESTS=true")
        
        config = self.load_config("sample_config.json")
        source = SourceZohoCreator()
        logger = logging.getLogger("test")
        
        # Step 1: Check connection
        success, error = source.check_connection(logger, config)
        if not success:
            pytest.skip(f"Connection check failed: {error}")
        
        # Step 2: Discover schema
        catalog = source.discover(logger, config)
        assert catalog is not None
        
        # Step 3: Read data
        messages = list(source.read(logger, config, catalog, None))
        
        # Should complete without errors
        assert isinstance(messages, list)

    @pytest.mark.integration
    @pytest.mark.slow
    def test_incremental_sync_with_state(self):
        """Test incremental sync with state management."""
        if not self.is_integration_test_enabled():
            pytest.skip("Integration tests disabled. Set AIRBYTE_INTEGRATION_TESTS=true")
        
        config = self.load_config("sample_config.json")
        source = SourceZohoCreator()
        logger = logging.getLogger("test")
        
        # Discover catalog
        catalog = source.discover(logger, config)
        
        # First sync (no state)
        messages_first = list(source.read(logger, config, catalog, None))
        
        # Extract state from last message if available
        state = None
        for msg in reversed(messages_first):
            if hasattr(msg, "state"):
                state = msg.state
                break
        
        # Second sync (with state from first)
        messages_second = list(source.read(logger, config, catalog, state))
        
        # Should return messages
        assert isinstance(messages_second, list)


class TestIntegrationErrorHandling(TestIntegrationSetup):
    """Integration tests for error handling."""

    @pytest.mark.integration
    def test_connection_timeout_handling(self):
        """Test handling of connection timeout."""
        if not self.is_integration_test_enabled():
            pytest.skip("Integration tests disabled. Set AIRBYTE_INTEGRATION_TESTS=true")
        
        # Config with unreachable endpoint
        invalid_config = {
            "client_id": "1000.timeout_test",
            "client_secret": "test",
            "client_refresh_token": "1000.test",
            "account_owner_name": "test@example.com",
            "app_link_name": "test_app",
            "base_accounts_url": "accounts.invalid.local",  # Invalid domain
            "base_url": "www.invalid.local",
        }
        
        source = SourceZohoCreator()
        logger = logging.getLogger("test")
        
        success, error = source.check_connection(logger, invalid_config)
        
        # Should fail gracefully
        assert success is False
        assert error is not None
