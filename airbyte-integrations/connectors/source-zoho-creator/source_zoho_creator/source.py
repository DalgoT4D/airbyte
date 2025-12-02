#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#

import logging
from typing import Any, Iterator, List, Mapping, MutableMapping, Optional

from airbyte_cdk.models import AirbyteMessage, ConfiguredAirbyteCatalog, SyncMode
from airbyte_cdk.sources import Source
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http.requests_native_auth import TokenAuthenticator

from .api import ZohoCreatorAPI
from .streams import ReportDataStream

logger = logging.getLogger("airbyte")


class SourceZohoCreator(Source):
    """
    Zoho Creator source connector implementation.
    
    This connector uses the Zoho Creator Data API to extract records from Zoho Creator applications.
    """

    def __init__(self, config: Mapping[str, Any]) -> None:
        """Initialize the Zoho Creator source with configuration."""
        super().__init__()
        # Store raw config and convenient attributes for later use
        self.config: Mapping[str, Any] = config
        self.client_id: str = config.get("client_id", "")
        self.client_secret: str = config.get("client_secret", "")
        self.client_refresh_token: str = config.get("client_refresh_token", "")
        self.account_owner_name: str = config.get("account_owner_name", "")
        self.app_link_name: Optional[str] = config.get("app_link_name")
        self.base_accounts_url: str = config.get("base_accounts_url", "")
        self.base_url: str = config.get("base_url", "")
    
    def check_connection(self, logger: logging.Logger, config: Mapping[str, Any]) -> tuple[bool, Optional[str]]:
        """
        Check if the connector can successfully connect to Zoho Creator API.
        
        Args:
            logger: Logger instance
            config: Configuration dictionary containing API credentials
            
        Returns:
            Tuple of (success: bool, error_message: Optional[str])
        """
        # Then in the check_connection method:
        try:
            api = ZohoCreatorAPI(
                client_id=config["client_id"],
                client_secret=config["client_secret"],
                client_refresh_token=config["client_refresh_token"],
                account_owner_name=config["account_owner_name"],
                app_link_name=config["app_link_name"],
                base_accounts_url=config["base_accounts_url"],
                base_url=config["base_url"],
            )

            success, error = api.validate_config()
        
            if success:
                logger.info("✅ Connection to Zoho Creator API successful")
            else:
                logger.error(f"❌ Connection failed: {error}")
        
            return success, error
        
        except KeyError as e:
            error_msg = f"Missing required configuration: {str(e)}"
            logger.error(error_msg)
            return False, error_msg
        except Exception as e:
            error_msg = f"Unexpected error during connection check: {str(e)}"
            logger.error(error_msg)
            return False, error_msg
        
    def discover(self, logger: logging.Logger, config: Mapping[str, Any]) -> ConfiguredAirbyteCatalog:
        """
        Discover available streams from Zoho Creator.
        
        Args:
            logger: Logger instance
            config: Configuration dictionary containing API credentials
            
        Returns:
            ConfiguredAirbyteCatalog with discovered streams
        """
        # TODO: Implement stream discovery
        # Should fetch available applications and forms from Zoho Creator API
        streams = self.streams(config)
        catalog = ConfiguredAirbyteCatalog([])
        return catalog

    def streams(self, config: Mapping[str, Any]) -> List:

        """Return stream instances based on configuration.
        This implementation instantiates a `ZohoCreatorAPI` with the
        required config fields and creates a `ReportDataStream` for each
        report discovered in the configured application.
        """
        api = ZohoCreatorAPI(
            client_id=config["client_id"],
            client_secret=config["client_secret"],
            client_refresh_token=config["client_refresh_token"],
            account_owner_name=config["account_owner_name"],
            app_link_name=config.get("app_link_name"),
            base_accounts_url=config["base_accounts_url"],
            base_url=config["base_url"],
        )

        stream_instances: List = []

        # Fetch reports for this application
        try:
            reports = api.get_application_reports()
        except Exception as e:
            logger.error(f"Failed to fetch reports: {e}")
            reports = []

        for report in reports:
            # each report obj returned by Zoho will contain `link_name` i.e. actual name of the report
            report_link_name = report.get("link_name")
            if not report_link_name:
                continue

            try:
                inst = ReportDataStream(
                    api=api,
                    report_link_name=report_link_name
                )
                stream_instances.append(inst)
            except Exception as e:
                logger.error(f"Failed to create ReportDataStream for report {report_link_name}: {e}")
                continue

        return stream_instances

    def read(
        self,
        logger: logging.Logger,
        config: Mapping[str, Any],
        catalog: ConfiguredAirbyteCatalog,
        state: Optional[MutableMapping[str, Any]] = None,
    ) -> Iterator[AirbyteMessage]:
        """
        Read data from Zoho Creator streams.
        
        Args:
            logger: Logger instance
            config: Configuration dictionary
            catalog: Catalog of streams to sync
            state: Current state for incremental syncs
            
        Yields:
            AirbyteMessage instances
        """
        # TODO: Implement data reading logic
        yield from []
