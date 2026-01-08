#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#


import sys
import traceback
import json
from datetime import datetime
from typing import List

from airbyte_cdk.entrypoint import AirbyteEntrypoint, launch, logger
from airbyte_cdk.exception_handler import init_uncaught_exception_handler
from airbyte_cdk.models import AirbyteErrorTraceMessage, AirbyteMessage, AirbyteTraceMessage, TraceType, Type
from source_zoho_creator import SourceZohoCreator


def _get_source(args: List[str]):
    try:
        # AbstractSource is instantiated without arguments
        return SourceZohoCreator()
    except Exception as error:
        # Use model_dump_json() for Pydantic v2 or json() for older versions
        try:
            error_msg = AirbyteMessage(
                type=Type.TRACE,
                trace=AirbyteTraceMessage(
                    type=TraceType.ERROR,
                    emitted_at=int(datetime.now().timestamp() * 1000),
                    error=AirbyteErrorTraceMessage(
                        message=f"Error starting the sync. This could be due to an invalid configuration or catalog. Please contact Support for assistance. Error: {error}",
                        stack_trace=traceback.format_exc(),
                    ),
                ),
            )
            # Try model_dump_json() first (Pydantic v2), fall back to json() (Pydantic v1)
            if hasattr(error_msg, 'model_dump_json'):
                print(error_msg.model_dump_json())
            else:
                print(error_msg.json())
        except Exception as serialization_error:
            # Fallback: just print the error message
            print(json.dumps({"type": "TRACE", "trace": {"type": "ERROR", "error": {"message": str(error)}}}))
        return None


def run():
    init_uncaught_exception_handler(logger)
    _args = sys.argv[1:]
    
    source = _get_source(_args)
    
    if source:
        launch(source, _args)
