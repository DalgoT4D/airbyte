#
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
#


import json
import re
from typing import Any, Dict, Iterable, List, Mapping, MutableMapping, Optional, Tuple
from abc import ABC
from urllib.parse import parse_qsl, urlparse
from datetime import datetime, timedelta, timezone

import requests
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import CheckpointMixin, Stream
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.models import SyncMode

stream_json_schema = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "type": "object",
    "additionalProperties": True,
    "properties": {
        "_id": {
            "type": [
                "number",
                "null",
            ]
        },
        "data": {
            "type": "object",
        },
        "endtime": {"type": ["string", "null"]},
        "end": {"type": ["string", "null"]},
        "_submission_time": {"type": ["string", "null"]},
    },
}


# pylint:disable=too-many-instance-attributes
class KoboToolStream(HttpStream, CheckpointMixin, ABC):
    """Each Kobo form is a stream"""

    primary_key = "_id"

    def __init__(
        self,
        config: Mapping[str, Any],
        form_id: str,
        schema: dict,
        name: str,
        pagination_limit: int,
        auth_token: str,
        include_labeled_outputs: bool = False,
        label_map: Optional[Mapping[str, str]] = None,
        choice_map: Optional[Mapping[str, Mapping[str, str]]] = None,
        select_field_types: Optional[Mapping[str, Mapping[str, str]]] = None,
        **kwargs,
    ):
        """constructor"""
        super().__init__()
        self.form_id = form_id
        self.auth_token = auth_token
        self.schema = schema
        self.stream_name = name
        self.base_url = config["base_url"]
        # pylint:disable=invalid-name
        self.PAGINATION_LIMIT = pagination_limit
        self._cursor_value = None
        # self.start_time = config["start_time"]
        self.start_time = config.get("start_time", "2023-03-15T00:00:00") 
        self.max_days_to_close = config.get("max_days_to_close", 30)
        self.exclude_fields = config["exclude_fields"] if "exclude_fields" in config else []
        self.include_labeled_outputs = include_labeled_outputs
        self.label_map = dict(label_map or {})
        self.choice_map = {
            list_name: dict(choices)
            for list_name, choices in (choice_map or {}).items()
        }
        self.select_field_types = {
            field_name: dict(field_type)
            for field_name, field_type in (select_field_types or {}).items()
        }

    @property
    def url_base(self) -> str:
        """base url for all http requests for kobo forms"""
        return f"{self.base_url}/api/v2/assets/{self.form_id}/"

    @property
    def name(self) -> str:
        """Return the english substring as stream name. If not found return form uid"""
        regex = re.compile("[^a-zA-Z0-9 ]")
        s = regex.sub("", getattr(self, 'stream_name', 'kobotoolstream'))
        s = s.strip()
        return s if len(s) > 0 else self.form_id

    def get_json_schema(self):
        """airbyte needs this function"""
        return self.schema

    @property
    def state(self) -> Mapping[str, Any]:
        """State will be a dict : {cursor_field: '2023-03-15T00:00:00.000+05:30'}"""
        retval = {}

        if self._cursor_value:
            retval[self.cursor_field] = self._cursor_value
        else:
            retval[self.cursor_field] = self.start_time

        return retval

    @state.setter
    def state(self, value: Mapping[str, Any]):
        """setter for state"""
        if self.cursor_field in value:
            self._cursor_value = value[self.cursor_field]

    def mk_tzaware_utc(self, dt: datetime):
        """
        add a utc-tzinfo object to the dt if it doesn't have tzinfo
        if it has a tzinfo, convert to utc
        """
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)

    def mk_query(self):
        """query using endtime"""
        retval = {}
        if self.cursor_field == "_submission_time":
            retval[self.cursor_field] = {"$gte": self.state[self.cursor_field]}

        else:
            start_sub_time = datetime.fromisoformat(self.state[self.cursor_field])
            start_sub_time -= timedelta(days=self.max_days_to_close)
            start_sub_time = self.mk_tzaware_utc(start_sub_time)
            tzaware_start_time = self.mk_tzaware_utc(datetime.fromisoformat(self.start_time))
            start_sub_time = max(start_sub_time, tzaware_start_time)
            retval[self.cursor_field] = {"$gte": self.state[self.cursor_field]}
            retval["_submission_time"] = {"$gte": start_sub_time.isoformat()}
        return retval

    def request_params(
        self,
        stream_state: Mapping[str, Any],  # pylint:disable=unused-argument
        stream_slice: Mapping[str, any] = None,  # pylint:disable=unused-argument
        next_page_token: Mapping[str, Any] = None,
    ) -> MutableMapping[str, Any]:
        """build the query request params"""
        sort_params = {}
        sort_params[self.cursor_field] = 1
        params = {"start": 0, "limit": self.PAGINATION_LIMIT, "sort": json.dumps(sort_params)}

        query = self.mk_query()

        params["query"] = json.dumps(query)

        if next_page_token:
            params.update(next_page_token)

        return params

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        """pagination"""
        json_response: Mapping[str, str] = response.json()
        next_url = json_response.get("next")
        params = None
        if next_url is not None:
            parsed_url = urlparse(next_url)
            params = dict(parse_qsl(parsed_url.query))
        return params

    def path(self, stream_slice: Mapping[str, Any] = None, **kwargs) -> str:  # pylint:disable=unused-argument
        """airbyte needs this function"""
        return "data.json"

    def request_headers(
        self,
        stream_state: Mapping[str, Any],  # pylint:disable=unused-argument
        stream_slice: Mapping[str, Any] = None,  # pylint:disable=unused-argument
        next_page_token: Mapping[str, Any] = None,  # pylint:disable=unused-argument
    ) -> Mapping[str, Any]:
        """build the request headers"""
        return {"Authorization": "Token " + self.auth_token}

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        """parse the response and yield the records"""
        json_response = response.json()
        result = json_response.get("results")

        for record in result:
            raw_record = dict(record)
            for to_remove_field in self.exclude_fields:
                if to_remove_field in raw_record:
                    raw_record.pop(to_remove_field)
            parsed_record = self._to_labeled_record(raw_record) if self.include_labeled_outputs else raw_record
            retval = {"_id": raw_record["_id"], "data": parsed_record}
            retval["_submission_time"] = raw_record["_submission_time"]
            retval["endtime"] = raw_record.get("endtime")
            retval["end"] = raw_record.get("end")
            if retval["endtime"]:
                # endtime is in utc
                endtime = self.mk_tzaware_utc(datetime.fromisoformat(retval["endtime"]))
                retval["endtime"] = endtime.isoformat()
            yield retval

    def _to_labeled_record(self, record: Mapping[str, Any]) -> Dict[str, Any]:
        """
        Relabel field names and select answers with form labels.
        Falls back to original keys/values when label metadata is unavailable.
        """
        labeled_record: Dict[str, Any] = {}

        for key, value in record.items():
            short_key = key.split("/")[-1]
            field_label = self.label_map.get(short_key, key)
            relabeled_value = self._to_labeled_value(short_key, value)

            # Avoid dropping data when two fields share the same human-readable label.
            final_label = field_label
            if final_label in labeled_record and final_label != key:
                final_label = f"{field_label} ({key})"

            labeled_record[final_label] = relabeled_value

        return labeled_record

    def _to_labeled_value(self, field_name: str, value: Any) -> Any:
        """Relabel select_one/select_multiple values using choice labels."""
        field_type = self.select_field_types.get(field_name)
        if not field_type or value in [None, ""]:
            return value

        list_name = field_type.get("list_name")
        if not list_name:
            return value

        list_choices = self.choice_map.get(list_name, {})
        if "select_multiple" in field_type.get("type", ""):
            return " ".join(list_choices.get(choice, choice) for choice in str(value).split())
        return list_choices.get(str(value), value)

    def read_records(
        self,
        sync_mode: SyncMode,
        cursor_field: List[str] | None = None,
        stream_slice: Mapping[str, Any] | None = None,
        stream_state: Mapping[str, Any] | None = None,
        **kwargs,
    ) -> Iterable[Mapping[str, Any]]:
        """read the records from the stream"""
        for record in super().read_records(sync_mode, cursor_field, stream_slice, stream_state, **kwargs):
            yield record
            if sync_mode == SyncMode.incremental:
                self._cursor_value = max(record[self.cursor_field], self._cursor_value) if self._cursor_value else record[self.cursor_field]


class KoboStreamSubmissionTime(KoboToolStream):
    """KoboStreamSubmissionTime"""

    cursor_field = "_submission_time"


class KoboStreamEndTime(KoboToolStream):
    """KoboStreamEndTime"""

    cursor_field = "endtime"


class KoboStreamEnd(KoboToolStream):
    """KoboStreamEnd"""

    cursor_field = "end"


class SourceKobotoolboxLab(AbstractSource):
    """One instance per sync"""

    # API_URL = "https://kf.kobotoolbox.org/api/v2"
    # TOKEN_URL = "https://kf.kobotoolbox.org/token/?format=json"
    PAGINATION_LIMIT = 30000

    @staticmethod
    def _extract_label(label_value: Any) -> Optional[str]:
        """Extract a single label from Kobo's label structures."""
        if isinstance(label_value, str):
            return label_value
        if isinstance(label_value, list):
            for entry in label_value:
                if isinstance(entry, str):
                    return entry
                if isinstance(entry, dict):
                    for dict_value in entry.values():
                        if isinstance(dict_value, str):
                            return dict_value
        if isinstance(label_value, dict):
            for dict_value in label_value.values():
                if isinstance(dict_value, str):
                    return dict_value
        return None

    def _build_form_label_metadata(
        self, asset: Mapping[str, Any]
    ) -> Tuple[Dict[str, str], Dict[str, Dict[str, str]], Dict[str, Dict[str, str]]]:
        """Build field and choice label maps from one form asset payload."""
        label_map: Dict[str, str] = {}
        choice_map: Dict[str, Dict[str, str]] = {}
        select_field_types: Dict[str, Dict[str, str]] = {}

        content = asset.get("content", {}) if isinstance(asset, dict) else {}
        survey_fields = content.get("survey", [])
        if not isinstance(survey_fields, list):
            survey_fields = []
        for field in survey_fields:
            if not isinstance(field, dict):
                continue
            name = field.get("$autoname") or field.get("name")
            if not name:
                continue

            label = self._extract_label(field.get("label"))
            if label:
                label_map[name] = label

            field_type = field.get("type", "")
            if isinstance(field_type, str) and ("select_one" in field_type or "select_multiple" in field_type):
                list_name = field.get("select_from_list_name")
                if isinstance(list_name, str) and list_name:
                    select_field_types[name] = {"type": field_type, "list_name": list_name}

        choices = content.get("choices", [])
        if not isinstance(choices, list):
            choices = []
        for choice in choices:
            if not isinstance(choice, dict):
                continue
            list_name = choice.get("list_name")
            choice_name = choice.get("name")
            if not list_name or choice_name is None:
                continue
            label = self._extract_label(choice.get("label")) or str(choice_name)
            choice_map.setdefault(str(list_name), {})[str(choice_name)] = label

        return label_map, choice_map, select_field_types

    def _get_form_label_metadata(
        self, config: Mapping[str, Any], form_id: str, auth_token: str
    ) -> Tuple[Dict[str, str], Dict[str, Dict[str, str]], Dict[str, Dict[str, str]]]:
        """Fetch one form definition and derive label metadata."""
        url = f"{config['base_url']}/api/v2/assets/{form_id}/"
        headers = {"Authorization": f"Token {auth_token}"}
        try:
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()
        except requests.exceptions.RequestException:
            return {}, {}, {}
        return self._build_form_label_metadata(response.json())

    def get_access_token(self, config) -> Tuple[str, any]:
        """get the access token for the given credentials"""
        token_url = f"{config['base_url']}/token/?format=json"
        auth = (config["username"], config["password"])
        try:
            response = requests.post(token_url, auth=auth, timeout=30)
            response.raise_for_status()
        except requests.exceptions.RequestException:
            return None, "error"

        json_response = response.json()
        if json_response is not None:
            return json_response.get("token"), None

        return None, "error"

    def check_connection(self, logger, config) -> Tuple[bool, any]:  # pylint:disable=unused-argument
        """check the connection with the credentials provided"""
        url = f"{config['base_url']}/api/v2/assets.json"
        auth = (config["username"], config["password"])
        response = requests.get(url, auth=auth, timeout=30)

        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError:
            return False, "Something went wrong. Please check your credentials"

        return True, None

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        """Fetch all assets(forms)"""
        url = f"{config['base_url']}/api/v2/assets.json"
        auth = (config["username"], config["password"])
        response = requests.get(url, auth=auth, timeout=30)
        json_response = response.json()
        key_list = json_response.get("results")
        include_labeled_outputs = config.get("include_labeled_outputs", False)

        # Generate a auth token for all streams
        auth_token, msg = self.get_access_token(config)  # pylint:disable=unused-variable
        if auth_token is None:
            return []

        # Generate array of stream objects
        streams = []
        for form_dict in key_list:
            if form_dict["has_deployment"]:
                label_map = {}
                choice_map = {}
                select_field_types = {}
                if include_labeled_outputs:
                    label_map, choice_map, select_field_types = self._get_form_label_metadata(
                        config=config, form_id=form_dict["uid"], auth_token=auth_token
                    )
                if "forms_using_endtime" in config and form_dict["name"] in config["forms_using_endtime"]:
                    stream = KoboStreamEndTime(
                        config=config,
                        form_id=form_dict["uid"],
                        schema=stream_json_schema,
                        name=form_dict["name"],
                        pagination_limit=self.PAGINATION_LIMIT,
                        auth_token=auth_token,
                        include_labeled_outputs=include_labeled_outputs,
                        label_map=label_map,
                        choice_map=choice_map,
                        select_field_types=select_field_types,
                    )
                elif "forms_using_end" in config and form_dict["name"] in config["forms_using_end"]:
                    stream = KoboStreamEnd(
                        config=config,
                        form_id=form_dict["uid"],
                        schema=stream_json_schema,
                        name=form_dict["name"],
                        pagination_limit=self.PAGINATION_LIMIT,
                        auth_token=auth_token,
                        include_labeled_outputs=include_labeled_outputs,
                        label_map=label_map,
                        choice_map=choice_map,
                        select_field_types=select_field_types,
                    )
                else:
                    stream = KoboStreamSubmissionTime(
                        config=config,
                        form_id=form_dict["uid"],
                        schema=stream_json_schema,
                        name=form_dict["name"],
                        pagination_limit=self.PAGINATION_LIMIT,
                        auth_token=auth_token,
                        include_labeled_outputs=include_labeled_outputs,
                        label_map=label_map,
                        choice_map=choice_map,
                        select_field_types=select_field_types,
                    )
                streams.append(stream)

        return streams

