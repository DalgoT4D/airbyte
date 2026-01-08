#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#

import logging

logger = logging.getLogger("airbyte")


class ZohoCreatorAPIError(Exception):
    """Exception for Zoho Creator API errors."""

    pass

class ZohoCreatorAuthError(ZohoCreatorAPIError):
    """Exception for authentication errors."""

    pass


class ZohoCreatorConfigError(Exception):
    """Exception for configuration errors."""

    pass
