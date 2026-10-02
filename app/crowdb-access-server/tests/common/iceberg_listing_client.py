# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.

"""Official-client request probe; the fixture is not a native listing endpoint."""

import json
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit

import pyarrow
import pyarrow.fs as fs
import pyiceberg
from pyiceberg.io.pyarrow import PyArrowFile


BUCKET = "native-catalog"
KEY = "t/table/data/file.parquet"
PAYLOAD = b"exact-file-payload"


class TestRequestHandler(BaseHTTPRequestHandler):
    def log_message(self, *_):
        pass

    def record(self):
        parsed = urlsplit(self.path)
        query = parse_qs(parsed.query)
        # Keep only semantic routing fields; never auth headers/query values.
        self.server.requests.append({
            "method": self.command,
            "path": parsed.path,
            "list_type": query.get("list-type", []),
            "prefix": query.get("prefix", []),
            "range": self.headers.get("Range"),
        })
        return parsed, query

    def do_HEAD(self):
        parsed, _ = self.record()
        if parsed.path == f"/{BUCKET}/{KEY}":
            self.send_response(200)
            self.send_header("Content-Length", str(len(PAYLOAD)))
            self.send_header("ETag", '"probe"')
            self.send_header("Last-Modified", "Sat, 03 Oct 2026 00:00:00 GMT")
        else:
            self.send_response(404)
            self.send_header("Content-Length", "0")
        self.end_headers()

    def do_GET(self):
        parsed, query = self.record()
        if query.get("list-type") == ["2"]:
            if self.server.reject_listing:
                body = b"<Error><Code>InvalidRequest</Code><Message>Listing unsupported</Message></Error>"
                self.send_response(400)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return
            prefix = query.get("prefix", [""])[0]
            contents = ""
            if KEY.startswith(prefix):
                contents = (
                    f"<Contents><Key>{KEY}</Key><Size>{len(PAYLOAD)}</Size>"
                    '<LastModified>2026-10-03T00:00:00Z</LastModified>'
                    '<ETag>"probe"</ETag><StorageClass>STANDARD</StorageClass></Contents>'
                )
            body = (
                '<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
                f"<Name>{BUCKET}</Name><Prefix>{prefix}</Prefix>"
                f"<IsTruncated>false</IsTruncated>{contents}</ListBucketResult>"
            ).encode()
            status = 200
        elif parsed.path == f"/{BUCKET}/{KEY}":
            body = PAYLOAD
            status = 200
            if self.headers.get("Range"):
                start, end = self.headers["Range"].removeprefix("bytes=").split("-")
                start, end = int(start), int(end) if end else len(PAYLOAD) - 1
                body = PAYLOAD[start:end + 1]
                status = 206
        else:
            body = b"<Error><Code>NoSuchKey</Code></Error>"
            status = 404
        self.send_response(status)
        self.send_header("Content-Length", str(len(body)))
        if status == 206:
            self.send_header("Content-Range", f"bytes {start}-{end}/{len(PAYLOAD)}")
        self.end_headers()
        self.wfile.write(body)


class TestListingCallSequences(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server = ThreadingHTTPServer(("127.0.0.1", 0), TestRequestHandler)
        cls.server.requests = []
        cls.server.reject_listing = False
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()
        cls.filesystem = fs.S3FileSystem(
            access_key="probe-access", secret_key="probe-secret", region="us-east-1",
            scheme="http", endpoint_override=f"127.0.0.1:{cls.server.server_port}",
            force_virtual_addressing=False,
        )

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.thread.join()

    def setUp(self):
        self.server.requests.clear()
        self.server.reject_listing = False

    def trace(self, operation):
        print(json.dumps({"pyiceberg": pyiceberg.__version__, "pyarrow": pyarrow.__version__,
            "operation": operation, "requests": self.server.requests}, ensure_ascii=False))

    def file(self, key=KEY):
        return PyArrowFile(f"s3://{BUCKET}/{key}", f"{BUCKET}/{key}", self.filesystem)

    def test_existing_exact_file(self):
        file = self.file()
        self.assertTrue(file.exists())
        self.assertEqual(len(file), len(PAYLOAD))
        with file.open() as stream:
            self.assertEqual(stream.read(), PAYLOAD)
        self.assertTrue(any(request["method"] == "HEAD" for request in self.server.requests))
        self.assertFalse(any(request["list_type"] for request in self.server.requests))
        self.trace("existing exact-file exists/length/open")

    def test_missing_exact_file(self):
        self.assertFalse(self.file("t/table/data/missing.parquet").exists())
        self.assertTrue(any(request["list_type"] == ["2"] for request in self.server.requests))
        self.trace("missing exact-file exists: incidental directory fallback")

    def test_intentional_prefix_selection(self):
        files = self.filesystem.get_file_info(fs.FileSelector(f"{BUCKET}/t/table/data", recursive=True))
        self.assertEqual([file.path for file in files], [f"{BUCKET}/{KEY}"])
        self.assertTrue(any(request["list_type"] == ["2"] for request in self.server.requests))
        self.trace("explicit Arrow FileSelector: intentional prefix discovery")

    def test_exact_creation_fails_when_incidental_listing_is_rejected(self):
        self.server.reject_listing = True
        with self.assertRaises(OSError):
            self.file("t/table/data/missing.parquet").create(overwrite=False)
        self.assertFalse(any(request["method"] == "PUT" for request in self.server.requests))
        self.assertTrue(any(request["list_type"] == ["2"] for request in self.server.requests))
        self.trace("exact create with listing rejected: failure before upload")


if __name__ == "__main__":
    unittest.main()
