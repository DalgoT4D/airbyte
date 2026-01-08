#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#

import logging
import time
from typing import Any, Dict, List, Mapping, Optional, Tuple
import requests

from airbyte_cdk.sources.streams.http.requests_native_auth import TokenAuthenticator

from .exceptions import ZohoCreatorAPIError, ZohoCreatorAuthError

logger = logging.getLogger("airbyte")


class ZohoCreatorAPI:
    """
    API client for Zoho Creator.
    
    This class handles authentication and API requests to the Zoho Creator API.
    """

    def __init__(self, client_id: str, client_secret: str, client_refresh_token: str, account_owner_name: str, app_link_name: str, base_accounts_url: str, base_url: str):
        """
        Initialize Zoho Creator API client.
        
        Args:
            client_id: Zoho OAuth Client ID
            client_secret: Zoho OAuth Client Secret
            client_refresh_token: Zoho OAuth Refresh Token
            account_owner_name: Zoho account owner username
            app_link_name: Target application link name
            base_accounts_url: Base URL for Zoho accounts (e.g., accounts.zoho.com)
            base_url: Base URL for Zoho Creator API (e.g., www.zohoapis.com)
        """
        self.client_id = client_id
        self.client_secret = client_secret
        self.client_refresh_token = client_refresh_token
        self.account_owner_name = account_owner_name
        self.app_link_name = app_link_name
        self.base_accounts_url = base_accounts_url
        self.base_url = base_url
        
        self._access_token: Optional[str] = None
        self._token_expires_at: Optional[float] = None

        logger.info(f"Initialized Zoho Creator API client for account: {self.account_owner_name}")
    
    def _get_token_endpoint(self) -> str:
        """Get the OAuth token endpoint based on base_accounts_url."""
        return f"https://{self.base_accounts_url}/oauth/v2/token"
    
    def _get_metadata_endpoint(self) -> str:
        """Return the metadata/test endpoint for the configured account and app."""
        return f"https://{self.base_url}/creator/v2.1/meta/{self.account_owner_name}/{self.app_link_name}/reports"

    def _get_data_endpoint(self, report_link_name: str) -> str:
        """Return the data endpoint for the configured account and app."""
        return f"https://{self.base_url}/creator/v2.1/data/{self.account_owner_name}/{self.app_link_name}/report/{report_link_name}"


    def get_access_token(self) -> str:
        """
        Get or refresh the access token for API authentication.
        
        Returns:
            Access token string

        Raises:
            ZohoCreatorAuthError: If token refresh fails
        """

        # if cached access token is still valid, then return it
        if self._access_token and self._token_expires_at:
         if time.time() < self._token_expires_at:
            return self._access_token
        
        # if access token is expired or not present, then get new token
        token_endpoint = self._get_token_endpoint()
        
        payload = {
            "grant_type": "refresh_token",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "refresh_token": self.client_refresh_token,
        }
        
        try:
            response = requests.post(token_endpoint, data=payload, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            
            if "access_token" not in data:
                raise ZohoCreatorAuthError(
                    f"Token endpoint did not return access_token. Response: {data}"
                )
            
            self._access_token = data["access_token"]
            
            # Cache expiry set using expires_in field in response
            if "expires_in" in data:
                expires_in = data.get("expires_in") - 60 #60 seconds buffer to ensure race condition is not hit
                if isinstance(expires_in, str):
                    expires_in = int(expires_in)
                self._token_expires_at = time.time() + expires_in
            
            logger.debug("Successfully refreshed access token")
            return self._access_token
            
        except requests.RequestException as e:
            raise ZohoCreatorAuthError(
                f"Failed to refresh access token: {str(e)}"
            ) from e


    def get_authenticator(self) -> TokenAuthenticator:
        """
        Get a TokenAuthenticator for use with CDK HttpStream.
        
        Returns:
            TokenAuthenticator: Configured authenticator
        """
        return TokenAuthenticator(
            token=self.get_access_token(),
            auth_method="Bearer",
        )

    def validate_config(self) -> Tuple[bool, Optional[str]]:
        """
        Validate that configuration can authenticate with API.
        
        Returns:
            Tuple of (success: bool, error_message: Optional[str])
        """
        try:
            # Get access token
            access_token = self.get_access_token()
            logger.info("Successfully obtained access token")
            
            # Try to make a test API call
            headers = {
                "Authorization": f"Bearer {access_token}"
            }
            
            url = self._get_metadata_endpoint()
            response = requests.get(url, headers=headers, timeout=10)
            
            # Check if the response is successful
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
                
        except ZohoCreatorAuthError as e:
            error_msg = f"Authentication error: {str(e)}"
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
        """Get list of all reports for given application using metadata API"""
        reports: List[dict] = []
        try:
            url = self._get_metadata_endpoint()
            headers = {"Authorization": f"Bearer {self.get_access_token()}"}
            response = requests.get(url, headers=headers, timeout=10)
            if response.status_code == 200:
                data = response.json()
                return data.get("reports", [])
        except Exception as e:
            logger.error(f"Failed to fetch reports: {e}")
            return []

    def get_report_data(self, report_link_name: str) -> List[dict]:
        """
        Get the data for a specific report.
        
        Args:
            report_link_name: Report link name
        """
        records = []
        try:
            url = self._get_data_endpoint(report_link_name)
            headers = {"Authorization": f"Bearer {self.get_access_token()}"}
            response = requests.get(url, headers=headers, timeout=10)
            if response.status_code == 200:
                response_json = response.json()
                records = response_json.get("data", [])
        except Exception as e:
            logger.error(f"Failed to fetch report data: {e}")
    
        return records

    def get_report_schema(self, report_link_name: str) -> Optional[Mapping[str, Any]]:
        """
        Get the schema for a specific report by inferring from sample data.
        
        Since Zoho Creator Data API returns all fields as strings, we extract
        field names from sample records and define all fields as string type.
        
        Args:
            report_link_name: Report link name
            
        Returns:
            Report schema dictionary with field names as keys and JSON Schema
            property definitions as values
        """
        properties: Dict[str, Dict[str, Any]] = {}
        
        try:
            # Fetch sample data (limit to first 20 records for efficiency)
            sample_records = self.get_report_data(report_link_name)[:20]
            
            if sample_records:
                # Extract all field names from sample records
                for record in sample_records:
                    for field_name, field_value in record.items():
                        # Skip if already processed
                        if field_name in properties:
                            continue
                        
                        # Determine type based on value structure
                        if isinstance(field_value, dict):
                            properties[field_name] = {"type": "object", "additionalProperties": True}
                        elif isinstance(field_value, list):
                            properties[field_name] = {"type": "array", "items": {"type": "string"}}
                        else:
                            # All scalar values are strings in Zoho Creator API
                            properties[field_name] = {"type": "string"}
                
                logger.debug(f"Generated schema for report {report_link_name} with {len(properties)} fields")
            else:
                logger.warning(f"No sample data found for report {report_link_name}, schema cannot be generated")
            
            return properties
            
        except Exception as e:
            logger.error(f"Failed to generate schema for report {report_link_name}: {e}")
            return {}
