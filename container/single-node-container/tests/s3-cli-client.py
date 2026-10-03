# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "app/crowdb-access-server/tests/s3_e2e"))
from clients import aws_workflow, rclone_workflow

os.environ["CROWDB_S3_E2E_ACCESS_KEY"] = os.environ["AWS_ACCESS_KEY_ID"]
os.environ["CROWDB_S3_E2E_SECRET_KEY"] = os.environ["AWS_SECRET_ACCESS_KEY"]
endpoint = os.environ["CROWDB_PREVIEW_S3_ENDPOINT"]
phase = sys.argv[1]
aws_workflow(endpoint, phase)
rclone_workflow(endpoint, phase)
