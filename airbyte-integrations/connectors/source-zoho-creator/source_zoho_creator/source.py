#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#

import logging
from typing import Any, Iterator, List, Mapping, MutableMapping, Optional

from airbyte_cdk.models import AirbyteMessage, AirbyteCatalog, SyncMode
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http.requests_native_auth import TokenAuthenticator

from .api import ZohoCreatorAPI
from .streams import ReportDataStream

logger = logging.getLogger("airbyte")


class SourceZohoCreator(AbstractSource):
    """
    Zoho Creator source connector implementation.
    
    This connector uses the Zoho Creator Data API to extract records from Zoho Creator applications.
    """
    
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
            app_link_name=config["app_link_name"],
            base_accounts_url=config["base_accounts_url"],
            base_url=config["base_url"],
        )

        stream_instances: List = []

        # Fetch reports for this application
        try:
            reports = api.get_application_reports()
            logger.info(f"Successfully fetched all report names for application {api.app_link_name}")
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

