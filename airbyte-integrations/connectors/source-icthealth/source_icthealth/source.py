#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


from abc import ABC
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple, Union
from urllib.parse import parse_qs, urlparse

import requests
from airbyte_cdk import SyncMode
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream


class IctHealthBaseStream(HttpStream, ABC):

    def __init__(
        self,
        name: str,
        path: str,
        auth_headers: dict,
        primary_key: Union[str, List[str]],
        data_field: str,
        to_date: str,
        from_date: str,
        **kwargs: Any,
    ) -> None:
        self._name = name
        self._path = path
        self.auth_headers = auth_headers
        self._primary_key = primary_key
        self._data_field = data_field
        self._to_date = to_date
        self._from_date = from_date
        self.page_size = kwargs.pop("page_size", 500)
        super().__init__(**kwargs)

    @property
    def url_base(self) -> str:
        return "https://ummeed.icthealth.com/"

    def request_params(
        self,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> MutableMapping[str, Any]:
        params = {
            "toDate": self._to_date,
            "fromDate": self._from_date,
            "pageSize": self.page_size,
            "pageNo": 0,
        }
        if next_page_token:
            params.update(next_page_token)

        return params

    def next_page_token(
        self, response: requests.Response
    ) -> Optional[Mapping[str, Any]]:
        try:
            query_params = parse_qs(urlparse(response.request.url).query)
            current_page = int(query_params.get("pageNo", [0])[0])

            # Server returns 200 but with no data_field key
            res = response.json()
            if self._data_field not in res or res.get("status") != "success":
                return None

            return {"pageNo": current_page + 1}
        except Exception as err:
            return None

    def request_headers(self, stream_state, stream_slice=None, next_page_token=None):
        return self.auth_headers

    def parse_response(
        self, response: requests.Response, **kwargs
    ) -> Iterable[Mapping]:
        response_json = response.json()
        if self._data_field:
            yield from response_json.get(self._data_field, [])
        else:
            yield from response_json

    @property
    def name(self) -> str:
        return self._name

    def path(
        self,
        *,
        stream_state: Optional[Mapping[str, Any]] = None,
        stream_slice: Optional[Mapping[str, Any]] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> str:
        return self._path

    @property
    def primary_key(self) -> Optional[Union[str, List[str], List[List[str]]]]:
        return self._primary_key


# Source
class SourceIctHealth(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        if not config.get("userName"):
            return False, "Missing userName in config"

        if not config.get("password"):
            return False, "Missing password in config"

        if not config.get("fromDate"):
            return False, "Missing fromDate in config"

        if not config.get("toDate"):
            return False, "Missing toDate in config"

        try:
            stream = IctHealthBaseStream(
                "ClinicEncounterData",
                "cmslive/portal/getClinicEncounterData/",
                {
                    "userName": config["userName"],
                    "password": config["password"],
                },
                "encounterid",
                "data",
                config["toDate"],
                config["fromDate"],
                page_size=10,
            )
            next(stream.read_records(sync_mode=SyncMode.full_refresh))
            return True, None
        except Exception as err:
            logger.error(f"Error establishing connection: {err}")
            return False, f"Error establishing connection: {err}"

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        return [
            IctHealthBaseStream(
                "ClinicEncounterData",
                "cmslive/portal/getClinicEncounterData/",
                {
                    "userName": config["userName"],
                    "password": config["password"],
                },
                "encounterid",
                "data",
                config["toDate"],
                config["fromDate"],
            ),
            IctHealthBaseStream(
                "RegisteredPatient",
                "cmslive/portal/registeredPatientDetails/",
                {
                    "userName": config["userName"],
                    "password": config["password"],
                },
                "id",
                "data",
                config["toDate"],
                config["fromDate"],
            ),
            IctHealthBaseStream(
                "AppointmentDetails",
                "cmslive/portal/getAppointmentList/",
                {
                    "userName": config["userName"],
                    "password": config["password"],
                },
                "eventid",
                "data",
                config["toDate"],
                config["fromDate"],
            ),
        ]
