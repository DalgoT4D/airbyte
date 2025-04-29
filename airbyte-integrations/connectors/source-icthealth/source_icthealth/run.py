#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


import sys

from airbyte_cdk.entrypoint import launch
from source_icthealth import SourceIctHealth


def run():
    source = SourceIctHealth()
    launch(source, sys.argv[1:])
