#
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
#


import json
import re
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple
from abc import ABC
from urllib.parse import parse_qsl, urlparse
from datetime import datetime, timedelta, timezone

import requests
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import CheckpointMixin, Stream
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.models import SyncMode

from .labeling import KoboLabelMaps, build_label_maps, label_submission

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
        "data_labeled": {"type": ["object", "null"]},
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
        # self.start_time = config.get("start_time") or "1970-01-01T00:00:00"
        self.start_time = config.get("start_time", "2023-03-15T00:00:00")
        self.max_days_to_close = config.get("max_days_to_close", 30)
        self.exclude_fields = config["exclude_fields"] if "exclude_fields" in config else []
        self.label_fields = bool(config.get("label_fields", False))
        self.label_language_index = int(config.get("label_language_index", 0))
        self._label_maps: Optional[KoboLabelMaps] = None

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

    def parse_datetime(self, value: str) -> datetime:
        if value.endswith("Z"):
            value = value[:-1] + "+00:00"
        return datetime.fromisoformat(value)

    def mk_query(self):
        """query using endtime"""
        retval = {}
        if self.cursor_field == "_submission_time":
            retval[self.cursor_field] = {"$gte": self.state[self.cursor_field]}

        else:
            start_sub_time = self.parse_datetime(self.state[self.cursor_field])
            start_sub_time -= timedelta(days=self.max_days_to_close)
            start_sub_time = self.mk_tzaware_utc(start_sub_time)
            tzaware_start_time = self.mk_tzaware_utc(self.parse_datetime(self.start_time))
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

    def _get_label_maps(self) -> Optional[KoboLabelMaps]:
        if not self.label_fields:
            return None
        if self._label_maps is not None:
            return self._label_maps

        url = f"{self.base_url}/api/v2/assets/{self.form_id}/"
        response = requests.get(url, headers={"Authorization": f"Token {self.auth_token}"}, timeout=30)
        response.raise_for_status()
        self._label_maps = build_label_maps(response.json(), language_index=self.label_language_index)
        return self._label_maps

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        """parse the response and yield the records"""
        json_response = response.json()
        result = json_response.get("results")

        for record in result:
            if not isinstance(record, Mapping):
                continue

            sanitized_record = dict(record)
            for to_remove_field in self.exclude_fields:
                sanitized_record.pop(to_remove_field, None)

            retval = {"_id": sanitized_record.get("_id"), "data": sanitized_record}
            retval["_submission_time"] = sanitized_record.get("_submission_time")
            retval["endtime"] = sanitized_record.get("endtime")
            retval["end"] = sanitized_record.get("end")
            if retval["endtime"]:
                # endtime is in utc
                endtime = self.mk_tzaware_utc(self.parse_datetime(retval["endtime"]))
                retval["endtime"] = endtime.isoformat()

            maps = self._get_label_maps()
            if maps is not None:
                retval["data_labeled"] = label_submission(sanitized_record, maps)
            yield retval

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


class SourceKobotoolbox(AbstractSource):
    """One instance per sync"""

    # API_URL = "https://kf.kobotoolbox.org/api/v2"
    # TOKEN_URL = "https://kf.kobotoolbox.org/token/?format=json"
    PAGINATION_LIMIT = 30000

    def get_access_token(self, config) -> Tuple[Optional[str], Any]:
        """Get the access token for the given credentials (or return provided API token)."""
        if config.get("token"):
            return config["token"], None

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
        if "base_url" not in config or not config["base_url"]:
            return False, "base_url is not provided"

        if config.get("token"):
            headers = {"Authorization": f"Token {config['token']}"}
            response = requests.get(f"{config['base_url']}/api/v2/assets.json", headers=headers, timeout=30)
        else:
            if "username" not in config:
                return False, "username in credentials is not provided"
            if "password" not in config:
                return False, "password in credentials is not provided"

            auth_token, msg = self.get_access_token(config)  # pylint:disable=unused-variable
            if auth_token is None:
                return False, "Something went wrong. Please check your credentials"
            headers = {"Authorization": f"Token {auth_token}"}
            response = requests.get(f"{config['base_url']}/api/v2/assets.json", headers=headers, timeout=30)

        try:
            response.raise_for_status()
        except requests.exceptions.RequestException:
            return False, "Something went wrong. Please check your credentials"

        return True, None

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        """Fetch all assets(forms)"""
        auth_token, msg = self.get_access_token(config)
        if auth_token is None:
            return []  # msg is surfaced via check_connection

        headers = {"Authorization": f"Token {auth_token}"}

        if config.get("asset_uid"):
            response = requests.get(f"{config['base_url']}/api/v2/assets/{config['asset_uid']}/", headers=headers, timeout=30)
            try:
                response.raise_for_status()
            except requests.exceptions.RequestException:
                return []
            asset = response.json()
            assets = [asset]
        else:
            response = requests.get(f"{config['base_url']}/api/v2/assets.json", headers=headers, timeout=30)
            try:
                response.raise_for_status()
            except requests.exceptions.RequestException:
                return []
            json_response = response.json()
            assets = json_response.get("results") or []

        # Generate array of stream objects
        streams = []
        for form_dict in assets:
            if not isinstance(form_dict, Mapping):
                continue
            if form_dict.get("has_deployment"):
                form_name = form_dict.get("name") or form_dict.get("uid") or "kobotoolstream"
                form_uid = form_dict.get("uid")
                if not form_uid:
                    continue

                if "forms_using_endtime" in config and form_name in config["forms_using_endtime"]:
                    stream = KoboStreamEndTime(
                        config=config,
                        form_id=form_uid,
                        schema=stream_json_schema,
                        name=form_name,
                        pagination_limit=self.PAGINATION_LIMIT,
                        auth_token=auth_token,
                    )
                elif "forms_using_end" in config and form_name in config["forms_using_end"]:
                    stream = KoboStreamEnd(
                        config=config,
                        form_id=form_uid,
                        schema=stream_json_schema,
                        name=form_name,
                        pagination_limit=self.PAGINATION_LIMIT,
                        auth_token=auth_token,
                    )
                else:
                    stream = KoboStreamSubmissionTime(
                        config=config,
                        form_id=form_uid,
                        schema=stream_json_schema,
                        name=form_name,
                        pagination_limit=self.PAGINATION_LIMIT,
                        auth_token=auth_token,
                    )
                streams.append(stream)

        return streams
