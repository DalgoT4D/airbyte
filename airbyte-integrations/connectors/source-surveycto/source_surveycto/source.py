#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


import base64
from abc import ABC
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple

import requests
from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import IncrementalMixin, Stream
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.utils.transform import TransformConfig, TypeTransformer


stream_json_schema = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "type": "object",
    "additionalProperties": True,
    "properties": {
        "KEY": {
            "type": [
                "string",
                "null",
            ]
        },
        "endtime": {"type": ["string", "null"]},
        "data": {
            "type": "object",
        },
        "SubmissionDate": {"type": ["string", "null"]},
    },
}


class SurveyStream(HttpStream, ABC):
    transformer: TypeTransformer = TypeTransformer(TransformConfig.DefaultSchemaNormalization)

    def __init__(self, config: Mapping[str, Any], form_id, schema, **kwargs):
        self.form_id = None
        super().__init__()

        self.config = config
        self.schema = schema
        self.server_name = config["server_name"]
        self.form_id = form_id
        self.start_date = config["start_date"]
        # base64 encode username and password as auth token
        user_name_password = f"{config['username']}:{config['password']}"
        self.auth_token = self._base64_encode(user_name_password)

    @property
    def url_base(self) -> str:
        return f"https://{self.server_name}.surveycto.com/" + "api/v2/forms/data/wide/json/"

    def _base64_encode(self, string: str) -> str:
        return base64.b64encode(string.encode("ascii")).decode("ascii")

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        return {}


class SurveyctoStream(SurveyStream, IncrementalMixin):
    primary_key = "KEY"
    cursor_field = "SubmissionDate"
    _cursor_value = None

    @property
    def state(self) -> Mapping[str, Any]:
        if self._cursor_value:
            return {self.cursor_field: self._cursor_value}
        else:
            return {self.cursor_field: self.start_date}

    @state.setter
    def state(self, value: Mapping[str, Any]):
        if self.cursor_field in value:
            self._cursor_value = value[self.cursor_field]

    @property
    def name(self) -> str:
        return self.form_id

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        return None

    def get_json_schema(self):
        return self.schema

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        ix = self.state[self.cursor_field]
        return {"date": ix}

    def request_headers(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> Mapping[str, Any]:
        return {"Authorization": "Basic " + self.auth_token}

    def path(self, stream_slice: Mapping[str, Any] = None, **kwargs) -> str:
        return self.form_id

    def parse_response(
        self,
        response: requests.Response,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> Iterable[Mapping]:
        self.response_json = response.json()

        for record in self.response_json:
            # send data, key, submission date and endtime
            record_id = record.get("KEY")
            submission_date = record.get("SubmissionDate")
            endtime = record.get("endtime")

            retval = {"KEY": record_id, "data": record}
            retval["SubmissionDate"] = submission_date
            retval["endtime"] = endtime

            yield retval

    def read_records(self, *args, **kwargs) -> Iterable[Mapping[str, Any]]:
        for record in super().read_records(*args, **kwargs):
            self._cursor_value = record[self.cursor_field]
            yield record


# Source
class SourceSurveycto(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, Any]:

        form_ids = config["form_id"]

        try:
            for form_id in form_ids:
                stream = SurveyctoStream(config=config, form_id=form_id, schema=stream_json_schema)
                next(stream.read_records(sync_mode=SyncMode.full_refresh))

            return True, None

        except Exception as error:
            return False, f"Unable to connect - {(error)}"

    def generate_streams(self, config: str) -> List[Stream]:
        forms = config.get("form_id", [])
        streams = []

        for form_id in forms:
            stream = SurveyctoStream(config=config, form_id=form_id, schema=stream_json_schema)
            streams.append(stream)
        return streams

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        streams = self.generate_streams(config=config)
        return streams
