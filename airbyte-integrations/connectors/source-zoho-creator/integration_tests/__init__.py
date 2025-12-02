#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#

from airbyte_cdk.models import SyncMode, AirbyteStream


def get_sample_config() -> dict:
    """
    Get sample configuration for testing.
    
    Returns:
        Sample configuration dictionary
    """
    return {
        "client_id": "sample_client_id",
        "client_secret": "sample_client_secret",
        "refresh_token": "sample_refresh_token",
        "data_center": "us",
    }


def get_invalid_config() -> dict:
    """
    Get invalid configuration for negative testing.
    
    Returns:
        Invalid configuration dictionary
    """
    return {
        "client_id": "",
        "client_secret": "",
        "refresh_token": "",
    }


def get_sample_catalog() -> dict:
    """
    Get sample catalog for testing.
    
    Returns:
        Sample catalog dictionary
    """
    return {
        "streams": [
            {
                "stream": {
                    "name": "sample_form",
                    "json_schema": {
                        "$schema": "http://json-schema.org/draft-07/schema#",
                        "type": "object",
                        "properties": {
                            "id": {"type": "string"},
                            "name": {"type": "string"},
                        },
                    },
                    "key_properties": ["id"],
                    "supported_sync_modes": [SyncMode.full_refresh.value],
                },
                "sync_mode": SyncMode.full_refresh.value,
                "destination_sync_mode": "overwrite",
            }
        ]
    }
