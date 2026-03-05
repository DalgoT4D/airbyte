#
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
#

from unittest.mock import Mock, patch

import requests

from source_kobotoolbox_lab.source import KoboStreamSubmissionTime, SourceKobotoolboxLab


def _base_config(include_labeled_outputs=False):
    return {
        "username": "username",
        "password": "password",
        "base_url": "https://kf.kobotoolbox.org",
        "start_time": "2023-03-15T00:00:00",
        "include_labeled_outputs": include_labeled_outputs,
    }


def test_build_form_label_metadata():
    source = SourceKobotoolboxLab()
    asset = {
        "content": {
            "survey": [
                {
                    "$autoname": "question_1",
                    "label": ["Favorite Fruit"],
                    "type": "select_one fruits",
                    "select_from_list_name": "fruits",
                },
                {
                    "name": "question_2",
                    "label": "Comments",
                    "type": "text",
                },
            ],
            "choices": [
                {"list_name": "fruits", "name": "apple", "label": ["Apple"]},
                {"list_name": "fruits", "name": "banana", "label": "Banana"},
            ],
        }
    }

    label_map, choice_map, select_field_types = source._build_form_label_metadata(asset)

    assert label_map == {"question_1": "Favorite Fruit", "question_2": "Comments"}
    assert choice_map == {"fruits": {"apple": "Apple", "banana": "Banana"}}
    assert select_field_types == {"question_1": {"type": "select_one fruits", "list_name": "fruits"}}


def test_streams_with_labeled_outputs_fetches_form_metadata():
    source = SourceKobotoolboxLab()
    config = _base_config(include_labeled_outputs=True)
    assets_response = Mock(spec=requests.Response)
    assets_response.json.return_value = {
        "results": [
            {"uid": "uid-1", "name": "Form A", "has_deployment": True},
            {"uid": "uid-2", "name": "Form B", "has_deployment": False},
        ]
    }
    label_map = {"q1": "Question 1"}
    choice_map = {"list_a": {"a": "Answer A"}}
    select_field_types = {"q1": {"type": "select_one list_a", "list_name": "list_a"}}

    with patch("source_kobotoolbox_lab.source.requests.get", return_value=assets_response), patch.object(
        source, "get_access_token", return_value=("token-123", None)
    ), patch.object(
        source, "_get_form_label_metadata", return_value=(label_map, choice_map, select_field_types)
    ) as metadata_mock:
        streams = source.streams(config)

    assert len(streams) == 1
    assert isinstance(streams[0], KoboStreamSubmissionTime)
    assert streams[0].include_labeled_outputs is True
    assert streams[0].label_map == label_map
    assert streams[0].choice_map == choice_map
    assert streams[0].select_field_types == select_field_types
    metadata_mock.assert_called_once_with(config=config, form_id="uid-1", auth_token="token-123")


def test_streams_without_labeled_outputs_skips_form_metadata():
    source = SourceKobotoolboxLab()
    config = _base_config(include_labeled_outputs=False)
    assets_response = Mock(spec=requests.Response)
    assets_response.json.return_value = {
        "results": [{"uid": "uid-1", "name": "Form A", "has_deployment": True}]
    }

    with patch("source_kobotoolbox_lab.source.requests.get", return_value=assets_response), patch.object(
        source, "get_access_token", return_value=("token-123", None)
    ), patch.object(source, "_get_form_label_metadata") as metadata_mock:
        streams = source.streams(config)

    assert len(streams) == 1
    assert streams[0].include_labeled_outputs is False
    metadata_mock.assert_not_called()
