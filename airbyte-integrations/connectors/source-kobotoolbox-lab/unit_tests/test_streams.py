#
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
#

from unittest.mock import Mock

import requests

from source_kobotoolbox_lab.source import KoboStreamSubmissionTime


def _base_config():
    return {
        "username": "user",
        "password": "pass",
        "base_url": "https://kf.kobotoolbox.org",
        "start_time": "2023-03-15T00:00:00",
    }


def _build_stream(include_labeled_outputs=False, **kwargs):
    return KoboStreamSubmissionTime(
        config=_base_config(),
        form_id="a1",
        schema={},
        name="form-name",
        pagination_limit=1000,
        auth_token="token-123",
        include_labeled_outputs=include_labeled_outputs,
        **kwargs,
    )


def test_parse_response_without_labels_preserves_raw_data():
    stream = _build_stream()
    response = Mock(spec=requests.Response)
    response.json.return_value = {
        "results": [
            {
                "_id": 7,
                "_submission_time": "2024-07-01T10:00:00+00:00",
                "group/question_1": "apple",
                "question_2": "red blue",
            }
        ]
    }

    parsed = list(stream.parse_response(response))

    assert parsed[0]["data"]["group/question_1"] == "apple"
    assert parsed[0]["data"]["question_2"] == "red blue"


def test_parse_response_with_labels_relabels_fields_and_choices():
    stream = _build_stream(
        include_labeled_outputs=True,
        label_map={"question_1": "Favorite Fruit", "question_2": "Colors"},
        choice_map={
            "fruits": {"apple": "Apple"},
            "colors": {"red": "Red", "blue": "Blue"},
        },
        select_field_types={
            "question_1": {"type": "select_one fruits", "list_name": "fruits"},
            "question_2": {"type": "select_multiple colors", "list_name": "colors"},
        },
    )
    response = Mock(spec=requests.Response)
    response.json.return_value = {
        "results": [
            {
                "_id": 7,
                "_submission_time": "2024-07-01T10:00:00+00:00",
                "group/question_1": "apple",
                "question_2": "red blue",
                "free_text": "hello",
            }
        ]
    }

    parsed = list(stream.parse_response(response))
    data = parsed[0]["data"]

    assert data["Favorite Fruit"] == "Apple"
    assert data["Colors"] == "Red Blue"
    assert data["free_text"] == "hello"
