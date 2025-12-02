#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#

import logging
from typing import Any, List, Mapping, Optional, Tuple
import requests

from airbyte_cdk.sources.streams.http.requests_native_auth import TokenAuthenticator

logger = logging.getLogger("airbyte")

class ZohoCreatorAPIError(Exception):
    """Base exception for Zoho Creator API errors."""
    pass


class ZohoCreatorAuthError(ZohoCreatorAPIError):
    """Exception for authentication errors."""
    pass

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
        # Check if cached access token is still valid (within 1 hour buffer)
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
            
            # Cache expiry (3600 seconds = 1 hour)
            if "expires_in" in data:
                import time
                expires_in = data.get("expires_in")
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
            # Try to refresh token
            access_token = self.get_access_token()
            logger.info("Successfully obtained access token")
            
            # Try to make a test API call
            headers = {
                "Authorization": f"Bearer {access_token}",
                "User-Agent": "Airbyte",
            }
            
            # Test endpoint: get application forms
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
        """Get reports for given application using metadata API"""
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

    def get_report_schema(self, report_link_name: str) -> Mapping[str, Any]:
        """
        Get the schema for a specific form.
        
        Args:
            report_link_name: Report link name
            
        Returns:
            Report schema dictionary
        """
        # TODO: Implement schema retrieval
        return {}
