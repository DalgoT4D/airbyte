#
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
#

from unittest.mock import Mock, patch

import json
import pytest
import requests

from source_kobotoolbox.source import KoboStreamSubmissionTime


def _make_stream(**overrides):
    config = {
        "base_url": "https://kf.kobotoolbox.org",
        "start_time": "2023-03-15T00:00:00",
        "exclude_fields": [],
    }
    config.update(overrides.get("config", {}))
    return KoboStreamSubmissionTime(
        config=config,
        form_id=overrides.get("form_id", "asset_uid_123"),
        schema=overrides.get("schema", {}),
        name=overrides.get("name", "my form"),
        pagination_limit=overrides.get("pagination_limit", 30000),
        auth_token=overrides.get("auth_token", "token_123"),
    )


def test_stream_url_base():
    stream = _make_stream(form_id="abc")
    assert stream.url_base == "https://kf.kobotoolbox.org/api/v2/assets/abc/"


def test_request_headers():
    stream = _make_stream(auth_token="t")
    assert stream.request_headers(stream_state={}) == {"Authorization": "Token t"}


def test_request_params_submission_time_cursor():
    stream = _make_stream()
    params = stream.request_params(stream_state={}, next_page_token=None)
    assert params["start"] == 0
    assert params["limit"] == 30000
    assert json.loads(params["sort"]) == {"_submission_time": 1}
    assert json.loads(params["query"]) == {"_submission_time": {"$gte": "2023-03-15T00:00:00"}}


@pytest.mark.parametrize(
    "next_url, expected",
    [
        ("https://kf.kobotoolbox.org/api/v2/assets/abc/data.json?start=200&limit=100", {"start": "200", "limit": "100"}),
        (None, None),
    ],
)
def test_next_page_token(next_url, expected):
    stream = _make_stream(form_id="abc")
    response = Mock(spec=requests.Response)
    response.json.return_value = {"next": next_url}
    assert stream.next_page_token(response) == expected


def test_parse_response_emits_labeled_data_when_enabled():
    stream = _make_stream(
        config={"label_fields": True, "label_language_index": 0, "exclude_fields": ["_notes"]},
        form_id="asset_uid_123",
        auth_token="t",
    )

    asset_definition = {
        "content": {
            "survey": [
                {"name": "q1", "label": ["Question 1"], "type": "text"},
                {"name": "choice_one", "label": ["One choice"], "type": "select_one", "select_from_list_name": "l1"},
                {"name": "choice_many", "label": ["Many choices"], "type": "select_multiple", "select_from_list_name": "l2"},
            ],
            "choices": [
                {"list_name": "l1", "name": "1", "label": ["Yes"]},
                {"list_name": "l1", "name": "2", "label": ["No"]},
                {"list_name": "l2", "name": "a", "label": ["Alpha"]},
                {"list_name": "l2", "name": "b", "label": ["Beta"]},
            ],
        }
    }

    data_page = {
        "results": [
            {
                "_id": 1,
                "_submission_time": "2023-03-16T00:00:00",
                "q1": "hello",
                "choice_one": "1",
                "choice_many": "a b",
                "_notes": "redact me",
            }
        ]
    }

    asset_response = Mock(spec=requests.Response)
    asset_response.raise_for_status.return_value = None
    asset_response.json.return_value = asset_definition

    page_response = Mock(spec=requests.Response)
    page_response.json.return_value = data_page

    with patch("source_kobotoolbox.source.requests.get", return_value=asset_response) as mock_get_asset:
        records = list(stream.parse_response(page_response))

    assert len(records) == 1
    record = records[0]
    assert record["data"].get("_notes") is None
    assert record["data_labeled"]["Question 1"] == "hello"
    assert record["data_labeled"]["One choice"] == "Yes"
    assert record["data_labeled"]["Many choices"] == "Alpha Beta"
    mock_get_asset.assert_called_once()

