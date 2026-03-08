#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#

import logging
import os
from typing import Any, Dict, List, Mapping, Optional, Tuple

import requests
from airbyte_cdk.sources.streams.http.requests_native_auth import TokenAuthenticator
from zoho_creator_sdk import ZohoCreatorClient

from .exceptions import ZohoCreatorAPIError

logger = logging.getLogger("airbyte")

# Maps datacenter code → API domain (used by HttpStream url_base in streams.py)
DATACENTER_API_URL: Dict[str, str] = {
    "US": "www.zohoapis.com",
    "EU": "www.zohoapis.eu",
    "IN": "www.zohoapis.in",
    "AU": "www.zohoapis.com.au",
    "CA": "www.zohoapis.ca",
    "JP": "www.zohoapis.jp"
}


class SDKRefreshingAuthenticator(TokenAuthenticator):
    """
    Token authenticator that fetches a fresh access token on every HTTP request.

    Replaces CDK's static TokenAuthenticator to prevent mid-sync 401 errors on
    long-running syncs where the OAuth2 token may expire (~1 hour for Zoho).

    The SDK auth handler caches the token internally and only calls the OAuth2
    token endpoint when the token is within its 60-second expiry buffer, so
    there is no network overhead on the vast majority of page requests.
    """

    def __init__(self, api: "ZohoCreatorAPI"):
        super().__init__(token="", auth_method="Bearer")
        self._api_ref = api

    @property
    def token(self) -> str:
        """Return the current access token, refreshing via SDK if near expiry."""
        return f"Bearer {self._api_ref.get_access_token()}"


class ZohoCreatorAPI:
    """
    API client for Zoho Creator.

    Wraps zoho_creator_sdk for authentication and config validation.
    Raw requests are used for report listing and data fetching (no SDK equivalent).
    """

    def __init__(
        self,
        client_id: str,
        client_secret: str,
        client_refresh_token: str,
        account_owner_name: str,
        app_link_name: str,
        datacenter: str = "US",
    ):
        self.client_id = client_id
        self.client_secret = client_secret
        self.client_refresh_token = client_refresh_token
        self.account_owner_name = account_owner_name
        self.app_link_name = app_link_name
        self.datacenter = datacenter.upper()
        self.base_url = DATACENTER_API_URL[self.datacenter]

        self._sdk_client = self._init_sdk_client()
        self._schema_cache: Dict[str, Dict[str, Any]] = {}
        logger.info(f"Initialized Zoho Creator API client for account: {self.account_owner_name}")

    def _init_sdk_client(self) -> ZohoCreatorClient:
        """
        Initialize the zoho_creator_sdk client via environment variables.

        The SDK's OAuth2AuthHandler requires redirect_uri at construction but does
        not include it in the token refresh payload, so a placeholder is used.
        """
        os.environ["ZOHO_CREATOR_CLIENT_ID"] = self.client_id
        os.environ["ZOHO_CREATOR_CLIENT_SECRET"] = self.client_secret
        os.environ["ZOHO_CREATOR_REFRESH_TOKEN"] = self.client_refresh_token
        os.environ["ZOHO_CREATOR_DATACENTER"] = self.datacenter
        os.environ["ZOHO_CREATOR_REDIRECT_URI"] = "https://localhost"
        return ZohoCreatorClient()

    def _get_metadata_endpoint(self) -> str:
        """Return the reports metadata endpoint for the configured app."""
        return f"https://{self.base_url}/creator/v2.1/meta/{self.account_owner_name}/{self.app_link_name}/reports"

    def _get_data_endpoint(self, report_link_name: str) -> str:
        """Return the data endpoint for a specific report."""
        return f"https://{self.base_url}/creator/v2.1/data/{self.account_owner_name}/{self.app_link_name}/report/{report_link_name}"

    def get_access_token(self) -> str:
        """
        Get a valid access token, refreshing via the SDK if needed.

        The SDK's OAuth2AuthHandler manages caching and expiry (with a 60-second
        buffer). Calling get_auth_headers() triggers a refresh when required.
        """
        self._sdk_client.auth_handler.get_auth_headers()
        return self._sdk_client.auth_handler.access_token

    def get_authenticator(self) -> SDKRefreshingAuthenticator:
        """Return a refreshing authenticator backed by the SDK auth handler."""
        return SDKRefreshingAuthenticator(self)

    def validate_config(self) -> Tuple[bool, Optional[str]]:
        """
        Validate credentials by calling the app's reports metadata endpoint.

        Uses /meta/{owner}/{app}/reports which requires ZohoCreator.meta.application.READ
        scope — the same scope required for normal connector operation.
        """
        try:
            access_token = self.get_access_token()
            headers = {"Authorization": f"Bearer {access_token}"}
            url = self._get_metadata_endpoint()
            response = requests.get(url, headers=headers, timeout=10)

            if response.status_code == 200:
                logger.info("Configuration validation successful")
                return True, None
            elif response.status_code == 401:
                error_msg = "Authentication failed. Please check your credentials."
                logger.error(error_msg)
                return False, error_msg
            elif response.status_code == 403:
                error_msg = "Access forbidden. Check if your account has permissions."
                logger.error(error_msg)
                return False, error_msg
            elif response.status_code == 404:
                error_msg = "Application or account not found. Check account_owner_name and app_link_name."
                logger.error(error_msg)
                return False, error_msg
            else:
                error_msg = f"API returned status {response.status_code}: {response.text[:200]}"
                logger.error(error_msg)
                return False, error_msg

        except requests.Timeout:
            error_msg = "Connection timeout. Check your network and API URLs."
            logger.error(error_msg)
            return False, error_msg
        except Exception as e:
            error_msg = f"Unexpected error during validation: {str(e)}"
            logger.error(error_msg)
            return False, error_msg

    def get_application_reports(self) -> List[dict]:
        """
        Get all reports for the configured application.

        The Zoho meta reports endpoint returns all reports in a single non-paginated
        response — no pagination parameters are accepted.
        """
        try:
            url = self._get_metadata_endpoint()
            headers = {"Authorization": f"Bearer {self.get_access_token()}"}
            response = requests.get(url, headers=headers, timeout=10)

            if response.status_code != 200:
                logger.error(
                    "Failed to fetch reports: HTTP %d — %s",
                    response.status_code, response.text[:200],
                )
                return []

            data = response.json()
            return data.get("reports", [])

        except Exception as e:
            logger.error(f"Failed to fetch reports: {e}")
            return []

    def get_report_data(self, report_link_name: str) -> List[dict]:
        """
        Fetch one page of records for a specific report via the HTTP data endpoint,
        used for schema inference during discover.

        Returns an empty list for reports with no records (Zoho code 3100) — this
        is a valid "empty" state. Non-200 HTTP responses are logged as errors so
        that permission/rate-limit issues are visible rather than silently producing
        an empty schema.
        """
        try:
            url = self._get_data_endpoint(report_link_name)
            headers = {"Authorization": f"Bearer {self.get_access_token()}"}
            response = requests.get(url, headers=headers, timeout=10)

            response_json = response.json()
            code = response_json.get("code")

            if code == 3000:
                return response_json.get("data", [])

            # Code 3100 = no records (arrives as HTTP 404) — valid empty state, not an error.
            if code == 3100:
                logger.info("Report '%s' has no records; schema will have no inferred fields.", report_link_name)
                return []

            if response.status_code != 200:
                logger.error(
                    "Failed to fetch sample data for report '%s': HTTP %d — %s",
                    report_link_name, response.status_code, response_json.get("message", response.text[:200]),
                )
                return []

            logger.warning(
                "Unexpected Zoho API code %s for report '%s' during schema inference: %s",
                code, report_link_name, response_json.get("message", ""),
            )
            return []

        except Exception as e:
            logger.error(f"Failed to fetch report data for '{report_link_name}': {e}")
            return []

    def get_report_schema(self, report_link_name: str) -> Optional[Mapping[str, Any]]:
        """
        Get the schema for a specific report by inferring from sample data.

        Since Zoho Creator Data API returns all fields as strings, we extract
        field names from sample records and define all fields as string type.
        """
        if report_link_name in self._schema_cache:
            return self._schema_cache[report_link_name]

        properties: Dict[str, Dict[str, Any]] = {}

        try:
            sample_records = self.get_report_data(report_link_name)[:20]

            if sample_records:
                for record in sample_records:
                    for field_name, field_value in record.items():
                        if field_name in properties:
                            continue

                        if isinstance(field_value, dict):
                            properties[field_name] = {"type": "object", "additionalProperties": True}
                        elif isinstance(field_value, list):
                            properties[field_name] = {"type": "array", "items": {"type": "string"}}
                        else:
                            properties[field_name] = {"type": "string"}

                logger.debug(f"Generated schema for report {report_link_name} with {len(properties)} fields")
            else:
                logger.warning(f"No sample data found for report {report_link_name}, schema cannot be generated")

            self._schema_cache[report_link_name] = properties
            return properties

        except Exception as e:
            logger.error(f"Failed to generate schema for report {report_link_name}: {e}")
            return {}
