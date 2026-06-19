#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


from typing import Any, Mapping, Optional
from airbyte_cdk.sources.file_based.config.excel_format import ExcelFormat
from airbyte_cdk.sources.file_based.file_based_stream_reader import FileReadMode

from airbyte_cdk import AdvancedAuth, ConfiguredAirbyteCatalog, ConnectorSpecification, OAuthConfigSpecification, TState
from airbyte_cdk.sources.file_based.file_based_source import FileBasedSource
from airbyte_cdk.sources.file_based.stream.cursor.default_file_based_cursor import DefaultFileBasedCursor
from source_microsoft_sharepoint.spec import SourceMicrosoftSharePointSpec
from source_microsoft_sharepoint.stream_reader import SourceMicrosoftSharePointStreamReader

import pandas as pd
from typing import Any, Dict, Iterator
# You may need to verify this exact import path by looking at the CDK source in your virtual environment
from airbyte_cdk.sources.file_based.file_types.file_type_parser import FileTypeParser


class AllSheetsExcelParser(FileTypeParser):
    @property
    def file_read_mode(self) -> FileReadMode:
        return FileReadMode.READ_BINARY

    def check_config(self, *args, **kwargs) -> bool:
        # Return True for success, and None for the error message
        return True, None
    
    async def infer_schema(
        self,
        config: Any,
        file: Any,
        stream_reader: Any,
        logger: Any,
        **kwargs
    ) -> dict:
        schema = {"_ab_source_file_sheet": {"type": "string"}}
        
        with stream_reader.open_file(file, self.file_read_mode, None, logger) as fp:
            try:
                # Tell Pandas to only peek at the tab matching the Stream Name
                df = pd.read_excel(fp, sheet_name=config.name, nrows=0)
            except ValueError:
                # If the tab doesn't exist in this specific file, return empty schema safely
                return schema
            
            # Register columns only for this specific tab
            for col in df.columns:
                schema[str(col)] = {"type": "string"}
                    
        return schema

    def parse_records(
        self,
        config: Any,
        file: Any,
        stream_reader: Any,
        logger: Any,
        discovered_schema: Any,
        *args,
        **kwargs
    ) -> Iterator[Dict[str, Any]]:
        
        with stream_reader.open_file(file, self.file_read_mode, None, logger) as fp:
            try:
                # Tell Pandas to only load data from the tab matching the Stream Name
                df = pd.read_excel(fp, sheet_name=config.name)
            except ValueError:
                logger.warning(f"Sheet '{config.name}' not found in {file.uri}. Skipping.")
                return

            df.dropna(how="all", inplace=True)
            df = df.where(pd.notnull(df), None)
            
            for record in df.to_dict(orient="records"):
                record["_ab_source_file_sheet"] = config.name
                
                # Forcefully cast ALL non-null values to strings to match our infer_schema!
                for key, value in record.items():
                    if value is not None:
                        # This safely handles Timestamps, huge ints, floats, and booleans
                        record[key] = str(value) 
                # ---------------
                
                yield record

class SourceMicrosoftSharePoint(FileBasedSource):
    def __init__(self, catalog: Optional[ConfiguredAirbyteCatalog], config: Optional[Mapping[str, Any]], state: Optional[TState]):
        super().__init__(
            stream_reader=SourceMicrosoftSharePointStreamReader(),
            spec_class=SourceMicrosoftSharePointSpec,
            catalog=catalog,
            config=config,
            state=state,
            cursor_cls=DefaultFileBasedCursor,
            parsers={ExcelFormat: AllSheetsExcelParser()}
        )

    def spec(self, *args: Any, **kwargs: Any) -> ConnectorSpecification:
        """
        Returns the specification describing what fields can be configured by a user when setting up a file-based source.
        """

        return ConnectorSpecification(
            documentationUrl=self.spec_class.documentation_url(),
            connectionSpecification=self.spec_class.schema(),
            advanced_auth=AdvancedAuth(
                auth_flow_type="oauth2.0",
                predicate_key=["credentials", "auth_type"],
                predicate_value="Client",
                oauth_config_specification=OAuthConfigSpecification(
                    complete_oauth_output_specification={
                        "type": "object",
                        "additionalProperties": False,
                        "properties": {"refresh_token": {"type": "string", "path_in_connector_config": ["credentials", "refresh_token"]}},
                    },
                    complete_oauth_server_input_specification={
                        "type": "object",
                        "additionalProperties": False,
                        "properties": {"client_id": {"type": "string"}, "client_secret": {"type": "string"}},
                    },
                    complete_oauth_server_output_specification={
                        "type": "object",
                        "additionalProperties": False,
                        "properties": {
                            "client_id": {"type": "string", "path_in_connector_config": ["credentials", "client_id"]},
                            "client_secret": {"type": "string", "path_in_connector_config": ["credentials", "client_secret"]},
                        },
                    },
                    oauth_user_input_from_connector_config_specification={
                        "type": "object",
                        "additionalProperties": False,
                        "properties": {"tenant_id": {"type": "string", "path_in_connector_config": ["credentials", "tenant_id"]}},
                    },
                ),
            ),
        )
