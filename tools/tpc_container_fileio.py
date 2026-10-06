# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Route container Iceberg FileIO through the explicitly selected endpoint."""

import os

from crowdb_tpc_loader.crowdb_fileio import CrowdbFileIO


class ContainerFileIO(CrowdbFileIO):
    def __init__(self, properties):
        properties = dict(properties)
        properties["s3.endpoint"] = os.environ["CROWDB_TPC_FILEIO_ENDPOINT"]
        super().__init__(properties)
