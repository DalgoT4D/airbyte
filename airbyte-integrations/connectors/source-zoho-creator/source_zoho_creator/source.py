#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#

import logging
from typing import Any, Iterator, List, Mapping, MutableMapping, Optional

from airbyte_cdk.models import AirbyteMessage, AirbyteCatalog, SyncMode
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from zoho_creator_sdk.exceptions import (
    AuthenticationError,
    ConfigurationError as SDKConfigurationError,
    InvalidCredentialsError,
    NetworkError,
    TokenRefreshError,
    ZohoCreatorError,
)

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

        Performs two checks in sequence:
        1. OAuth2 token retrieval — surfaces typed SDK auth exceptions.
        2. HTTP metadata endpoint call — verifies app/owner access and API reachability.

        Returns:
            Tuple of (success: bool, error_message: Optional[str])
        """
        try:
            api = ZohoCreatorAPI(
                client_id=config["client_id"],
                client_secret=config["client_secret"],
                client_refresh_token=config["client_refresh_token"],
                account_owner_name=config["account_owner_name"],
                app_link_name=config["app_link_name"],
                datacenter=config.get("datacenter", "US"),
            )
        except KeyError as e:
            error_msg = f"Missing required configuration field: {e}"
            logger.error(error_msg)
            return False, error_msg
        except SDKConfigurationError as e:
            error_msg = f"SDK configuration error (check credentials format): {e}"
            logger.error(error_msg)
            return False, error_msg

        # Call get_access_token() directly so typed SDK exceptions surface here
        # rather than being swallowed by validate_config()'s generic except clause.
        # validate_config() will reuse the cached token on its own internal call.
        try:
            api.get_access_token()
        except InvalidCredentialsError as e:
            error_msg = f"Invalid credentials: client_id, client_secret, or refresh token is incorrect. ({e})"
            logger.error(error_msg)
            return False, error_msg
        except TokenRefreshError as e:
            recoverable = "may succeed on retry" if e.is_recoverable else "check your refresh token"
            error_msg = f"Token refresh failed ({recoverable}): {e}"
            logger.error(error_msg)
            return False, error_msg
        except AuthenticationError as e:
            error_msg = f"Authentication failed: {e}"
            logger.error(error_msg)
            return False, error_msg
        except NetworkError as e:
            error_msg = f"Network error while reaching Zoho OAuth endpoint: {e}"
            logger.error(error_msg)
            return False, error_msg
        except ZohoCreatorError as e:
            error_msg = f"Zoho SDK error during authentication: {e}"
            logger.error(error_msg)
            return False, error_msg

        # HTTP-level check: validates app_link_name, account_owner_name, and API reachability.
        success, error = api.validate_config()

        if success:
            logger.info("Connection to Zoho Creator API successful")
        else:
            logger.error(f"Connection failed: {error}")

        return success, error

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
            datacenter=config.get("datacenter", "US"),
        )

        stream_instances: List = []

        # Fetch reports for this application
        try:
            reports = api.get_application_reports()
            if reports:
                logger.info(f"Successfully fetched all report names for application {api.app_link_name}")
                # debugging
                print(reports)
            else:
                logger.warning(f"No reports found for application {api.app_link_name}")
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

