#
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
#

from unittest.mock import Mock, patch

import pytest
from source_kobotoolbox.source import SourceKobotoolbox


def test_check_connection_missing_base_url():
    ok, msg = SourceKobotoolbox().check_connection(logger=None, config={"token": "t"})
    assert (ok, msg) == (False, "base_url is not provided")


@pytest.mark.parametrize(
    "config, expected",
    [
        ({"base_url": "https://kf.kobotoolbox.org", "password": "some_password"}, (False, "username in credentials is not provided")),
        ({"base_url": "https://kf.kobotoolbox.org", "username": "username"}, (False, "password in credentials is not provided")),
    ],
)
def test_check_connection_missing_basic_credentials(config, expected):
    ok, msg = SourceKobotoolbox().check_connection(logger=None, config=config)
    assert (ok, msg) == expected


def test_check_connection_token_auth_success():
    response = Mock()
    response.raise_for_status.return_value = None
    with patch("source_kobotoolbox.source.requests.get", return_value=response) as mock_get:
        ok, msg = SourceKobotoolbox().check_connection(
            logger=None, config={"base_url": "https://kf.kobotoolbox.org", "token": "t"}
        )
    assert (ok, msg) == (True, None)
    mock_get.assert_called_once()


def test_streams_returns_empty_when_token_fetch_fails():
    with patch.object(SourceKobotoolbox, "get_access_token", return_value=(None, "error")):
        streams = SourceKobotoolbox().streams({"base_url": "https://kf.kobotoolbox.org", "username": "u", "password": "p"})
    assert streams == []
