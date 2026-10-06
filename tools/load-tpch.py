#!/usr/bin/env python3
# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
"""Load TPC-H into local Iceberg, then upload the same Parquet files to S3.

Run: pixi run -e iceberg-e2e python tools/load-tpch.py
Local files and both upload reports remain under .crowdb-runtime/artifacts/.
"""

import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
from datetime import datetime, timezone
from urllib.request import Request, urlopen
from uuid import uuid4

REPO = Path(__file__).resolve().parents[1]
ARTIFACTS = REPO / ".crowdb-runtime/artifacts/tpc-loader"


def arguments():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--console-url", default="http://127.0.0.1:9090")
    parser.add_argument("--sf", default="1", help="TPC-H scale factor (default: 1)")
    parser.add_argument("--namespace", help="new Iceberg namespace; default is a unique tpch_sf1_* name")
    parser.add_argument("--bucket", help="S3 bucket; default is the namespace with underscores replaced by hyphens")
    parser.add_argument("--catalog-uri", help="override the cluster Iceberg endpoint")
    parser.add_argument("--s3-endpoint", help="override the cluster S3 endpoint")
    parser.add_argument("--credentials-file", type=Path, default=REPO / ".crowdb-runtime/persistent/console/default/secrets/client.env")
    parser.add_argument("--work-dir", type=Path, help="parent directory for retained Parquet and reports")
    parser.add_argument("--load-report", type=Path, help="upload files from an existing successful loader report instead of loading again")
    parser.add_argument("--prepare-only", action="store_true", help="install the loader without connecting or loading data")
    parser.add_argument("--check-only", action="store_true", help="install and check live Iceberg/S3 access without writing data")
    parser.add_argument("--installed", action="store_true", help=argparse.SUPPRESS)
    return parser.parse_args()


def bootstrap(args):
    if args.installed:
        return
    if not (3, 10) <= sys.version_info[:2] < (3, 13):
        raise RuntimeError("Use Python 3.10–3.12: pixi run -e iceberg-e2e python tools/load-tpch.py")
    python = ARTIFACTS / "venv/bin/python"
    if not python.exists():
        python.parent.parent.parent.mkdir(parents=True, exist_ok=True)
        subprocess.run([sys.executable, "-m", "venv", str(python.parent.parent)], check=True)
    subprocess.run([str(python), "-m", "pip", "install", "crowdb-tpc-loader"], check=True)
    result = subprocess.run([str(python), str(Path(__file__).resolve()), *sys.argv[1:], "--installed"])
    raise SystemExit(result.returncode)


def credentials(path):
    values = {}
    if path.exists():
        for line in path.read_text().splitlines():
            if line and not line.startswith("#"):
                key, value = line.split("=", 1)
                values[key] = value
    for key in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN", "AWS_DEFAULT_REGION", "ICEBERG_TOKEN"):
        if key in os.environ:
            values[key] = os.environ[key]
    missing = [key for key in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "ICEBERG_TOKEN") if not values.get(key)]
    if missing:
        raise RuntimeError(f"Missing {', '.join(missing)}; provide --credentials-file or environment variables")
    return values


def get_json(url, token=None):
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    with urlopen(Request(url, headers=headers), timeout=60) as response:
        return json.load(response)


def connect(args):
    from crowdb_tpc_loader.upload_client import UploadClient

    values = credentials(args.credentials_file)
    endpoints = get_json(args.console_url.rstrip("/") + "/api/access/connections")
    catalog = args.catalog_uri or os.environ.get("ICEBERG_URI") or endpoints.get("iceberg")
    endpoint = args.s3_endpoint or os.environ.get("AWS_ENDPOINT_URL") or endpoints.get("s3")
    if not catalog or not endpoint:
        raise RuntimeError("The cluster must have ready Iceberg and S3 Access services")
    get_json(catalog.rstrip("/") + "/v1/config", values["ICEBERG_TOKEN"])
    properties = {
        "s3.endpoint": endpoint,
        "s3.access-key-id": values["AWS_ACCESS_KEY_ID"],
        "s3.secret-access-key": values["AWS_SECRET_ACCESS_KEY"],
        "client.region": values.get("AWS_DEFAULT_REGION", "us-east-1"),
    }
    if values.get("AWS_SESSION_TOKEN"):
        properties["s3.session-token"] = values["AWS_SESSION_TOKEN"]
    transport = UploadClient(timeout=60)
    try:
        client = transport.bind(properties)
        client.list_buckets()
    except BaseException:
        transport.close()
        raise
    print(f"Iceberg: {catalog}\nS3: {endpoint}", flush=True)
    return catalog, values["ICEBERG_TOKEN"], properties, transport, client


def load(args, catalog, token, work):
    if args.load_report:
        return args.load_report.resolve()
    report = work / "iceberg-report.json"
    env = {**os.environ, "ICEBERG_URI": catalog, "ICEBERG_TOKEN": token}
    subprocess.run([
        sys.executable, "-m", "crowdb_tpc_loader", "load", "--benchmark", "tpch",
        "--sf", args.sf, "--namespace", args.namespace, "--work-dir", str(work),
        "--keep-files", "--report-file", str(report),
    ], env=env, check=True)
    return report


def save_report(path, data):
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(data, indent=2) + "\n")
    temporary.replace(path)


def copy_objects(args, report_path, work, properties, client):
    from botocore.exceptions import ClientError
    from crowdb_tpc_loader.s3_upload import file_checksums, upload_file

    report = json.loads(report_path.read_text())
    tables = report["tables"]
    if report["status"] != "succeeded" or len(tables) != 8 or any(table["status"] != "succeeded" for table in tables.values()):
        raise RuntimeError("S3 copy requires a successful report containing all eight TPC-H tables")
    root = Path(report["work_directory"]).resolve()
    bucket = args.bucket or "-".join(report["namespace"]).replace("_", "-")
    try:
        client.create_bucket(Bucket=bucket)
    except ClientError as error:
        if error.response["Error"]["Code"] != "BucketAlreadyOwnedByYou":
            raise
    copied = {"iceberg_report": str(report_path), "bucket": bucket, "status": "running", "objects": []}
    output = work / "s3-report.json"
    save_report(output, copied)
    for table in tables.values():
        for item in table["files"]:
            source = (root / item["local_path"]).resolve()
            if not source.is_relative_to(root):
                raise RuntimeError("Loader file path is outside its staging directory")
            size, md5, parts = file_checksums(source, item["size_bytes"], 8 * 1024 * 1024)
            if size != item["size_bytes"] or md5 != item["md5"]:
                raise RuntimeError(f"Parquet changed after its Iceberg upload: {source}")
            key = source.relative_to(root).as_posix()
            uri = f"s3://{bucket}/{key}"
            print(f"Copy {uri} ({size:,} bytes)", flush=True)
            upload_file(properties, uri, source, size, lambda _: None, md5=md5, client=client, part_digests=parts)
            head = client.head_object(Bucket=bucket, Key=key)
            if head["ContentLength"] != size:
                raise RuntimeError(f"S3 object size mismatch: {uri}")
            copied["objects"].append({"iceberg_uri": item["remote_uri"], "s3_uri": uri, "size_bytes": size, "md5": md5})
            save_report(output, copied)
    copied["status"] = "succeeded"
    save_report(output, copied)
    print(f"Loaded eight Iceberg tables and copied {len(copied['objects'])} identical Parquet objects.\nIceberg report: {report_path}\nS3 report: {output}")


def main():
    args = arguments()
    bootstrap(args)
    if args.prepare_only:
        subprocess.run([sys.executable, "-m", "crowdb_tpc_loader", "--version"], check=True)
        return
    catalog, token, properties, transport, client = connect(args)
    try:
        if args.check_only:
            print("Iceberg catalog and signed S3 access are ready; no data written.")
            return
        stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S") + "_" + uuid4().hex[:8]
        args.namespace = args.namespace or f"tpch_sf{args.sf.replace('.', '_')}_{stamp}"
        work = (args.work_dir or ARTIFACTS / args.namespace).resolve()
        work.mkdir(parents=True, exist_ok=True)
        report = load(args, catalog, token, work)
        copy_objects(args, report, work, properties, client)
    finally:
        transport.close()


if __name__ == "__main__":
    try:
        main()
    except (OSError, RuntimeError, subprocess.CalledProcessError) as error:
        print(f"Error: {error}", file=sys.stderr)
        raise SystemExit(1) from None
