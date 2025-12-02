#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#

from typing import Any, Mapping, Optional


def get_zone_url(zone: str) -> str:
    """
    Get the base URL for the Zoho API based on the zone/data center.
    
    Args:
        zone: Data center zone (us, eu, au, in, etc.)
        
    Returns:
        Base URL for API calls
    """
    zone_map = {
        "us": "https://www.zohoapis.com",
        "eu": "https://www.zohoapis.eu",
        "au": "https://www.zohoapis.com.au",
        "in": "https://www.zohoapis.in",
        "cn": "https://www.zohoapis.com.cn",
        "jp": "https://www.zohoapis.jp",
    }
    return zone_map.get(zone.lower(), "https://www.zohoapis.com")


def normalize_config(config: Mapping[str, Any]) -> Mapping[str, Any]:
    """
    Normalize and validate connector configuration.
    
    Args:
        config: Configuration dictionary
        
    Returns:
        Normalized configuration
    """
    # TODO: Implement config normalization
    return config
