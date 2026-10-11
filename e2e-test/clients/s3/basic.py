# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.

import unittest
from base import S3ClientTestCase
from batch_delete import BatchDeleteCases
from default_client import DefaultClientCases
from clients import CliClientCases
from user_metadata import UserMetadataCases
from wire import WireCases
from multipart import MultipartCases
from concurrency import ConcurrencyCases
from object_copy import CopyCases
from upload import UploadCases
from objects import ObjectsCases


class BasicS3CompatibilityTest(WireCases, MultipartCases, ConcurrencyCases, CopyCases, UploadCases, ObjectsCases, BatchDeleteCases, DefaultClientCases, CliClientCases, UserMetadataCases, S3ClientTestCase):
    pass


if __name__ == "__main__":
    unittest.main()
