# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.

from base import *


class WireCases:
    def test_signed_raw_http_wire_contract(self):
        bucket = f"{self.bucket}-raw"
        key = "raw/%25+边界.bin"
        path = f"/{bucket}/{quote(key, safe='/')}"
        payload = b"raw-http-object\x00payload"

        status, _, _ = self.signed_http("PUT", f"/{bucket}")
        self.assertEqual(status, 200)
        status, _, data = self.signed_http("HEAD", f"/{bucket}")
        self.assertEqual(status, 200)
        self.assertEqual(data, b"")
        status, _, data = self.signed_http("GET", "/")
        self.assertEqual(status, 200)
        self.assertIn(bucket.encode(), data)
        status, headers, data = self.signed_http("PUT", path, payload)
        self.assertEqual(status, 200, data)
        self.assertEqual(headers["etag"], f'"{md5(payload).hexdigest()}"')
        status, headers, data = self.signed_http("GET", path, headers={"Range": "bytes=4-10"})
        self.assertEqual(status, 206)
        self.assertEqual(headers["content-range"], f"bytes 4-10/{len(payload)}")
        self.assertEqual(data, payload[4:11])
        status, _, data = self.signed_http("GET", path, headers={"Range": "bytes=-4"})
        self.assertEqual(status, 206)
        self.assertEqual(data, payload[-4:])
        status, _, data = self.signed_http("GET", path, headers={"Range": "bytes=4-"})
        self.assertEqual(status, 206)
        self.assertEqual(data, payload[4:])
        status, headers, data = self.signed_http("HEAD", path)
        self.assertEqual(status, 200)
        self.assertEqual(int(headers["content-length"]), len(payload))
        self.assertEqual(data, b"")
        status, _, data = self.signed_http("GET", path, headers={"If-None-Match": headers["etag"]})
        self.assertEqual(status, 304)
        self.assertEqual(data, b"")
        modified = self.client.head_object(Bucket=bucket, Key=key)["LastModified"].astimezone(timezone.utc)
        status, _, data = self.signed_http(
            "GET", path, headers={"If-Modified-Since": format_datetime(modified + timedelta(days=1), usegmt=True)}
        )
        self.assertEqual(status, 304)
        self.assertEqual(data, b"")
        status, _, data = self.signed_http(
            "GET", path, headers={"If-Unmodified-Since": format_datetime(modified - timedelta(days=1), usegmt=True)}
        )
        self.assertEqual(status, 412)
        self.assertIn(b"PreconditionFailed", data)
        status, _, data = self.signed_http("GET", path, headers={"If-Match": '"wrong"'})
        self.assertEqual(status, 412)
        self.assertIn(b"PreconditionFailed", data)
        status, _, data = self.signed_http("GET", path, headers={"Range": "bytes=999-1000"})
        self.assertEqual(status, 416)
        self.assertIn(b"InvalidRange", data)
        status, _, data = self.signed_http("GET", path, headers={"Range": "bytes=0-1,4-5"})
        self.assertEqual(status, 416)
        self.assertIn(b"InvalidRange", data)
        status, _, data = self.signed_http("GET", f"{path}?versionId=1")
        self.assertEqual(status, 501)
        self.assertIn(b"NotImplemented", data)
        status, _, data = self.signed_http("GET", path, corrupt_signature=True)
        self.assertEqual(status, 403)
        self.assertIn(b"AccessDenied", data)
        status, _, data = self.signed_http("GET", f"/{bucket}?list-type=2&prefix=raw%2F")
        self.assertEqual(status, 200)
        self.assertIn(b"<Key>raw/%25+", data)
        second_path = f"/{bucket}/raw/second.bin"
        status, _, _ = self.signed_http("PUT", second_path, b"second")
        self.assertEqual(status, 200)
        status, _, data = self.signed_http("GET", f"/{bucket}?list-type=2&max-keys=1")
        self.assertEqual(status, 200)
        first_page = ElementTree.fromstring(data)
        token = first_page.findtext(".//{*}NextContinuationToken")
        self.assertTrue(token)
        status, _, data = self.signed_http(
            "GET", f"/{bucket}?list-type=2&max-keys=1&continuation-token={quote(token, safe='')}"
        )
        self.assertEqual(status, 200)
        second_page = ElementTree.fromstring(data)
        self.assertEqual(second_page.findtext(".//{*}Key"), "raw/second.bin")
        status, _, data = self.signed_http("GET", f"/{bucket}?list-type=2&continuation-token=invalid")
        self.assertEqual(status, 400)
        self.assertIn(b"InvalidRequest", data)
        status, _, _ = self.signed_http("DELETE", path)
        self.assertEqual(status, 204)
        status, _, _ = self.signed_http("DELETE", path)
        self.assertEqual(status, 204)
        status, _, data = self.signed_http("GET", path)
        self.assertEqual(status, 404)
        self.assertIn(b"NoSuchKey", data)
        status, _, _ = self.signed_http("DELETE", second_path)
        self.assertEqual(status, 204)
        status, _, _ = self.signed_http("DELETE", f"/{bucket}")
        self.assertEqual(status, 204)

    def test_fragmentation_and_storage_boundaries(self):
        client = self.client
        bucket = f"{self.bucket}-boundaries"
        client.create_bucket(Bucket=bucket)
        sizes = [0, 1, 65505, 65506, 65507, 1024 * 1024 + 31, 4 * 1024 * 1024 + 127]
        keys = []
        for index, size in enumerate(sizes):
            key = f"encoded/边界-{index}-%00.bin"
            payload = bytes((offset * 31 + index) % 256 for offset in range(size))
            digest = md5(payload)
            result = client.put_object(
                Bucket=bucket,
                Key=key,
                Body=FragmentedBody(payload, index + 17),
                ContentLength=size,
                ContentMD5=b64encode(digest.digest()).decode("ascii"),
            )
            self.assertEqual(result["ETag"], f'"{digest.hexdigest()}"')
            fetched = client.get_object(Bucket=bucket, Key=key)["Body"].read()
            self.assertEqual(fetched, payload)
            if size:
                start = min(size - 1, 65500)
                end = min(size - 1, start + 31)
                ranged = client.get_object(
                    Bucket=bucket,
                    Key=key,
                    Range=f"bytes={start}-{end}",
                )["Body"].read()
                self.assertEqual(ranged, payload[start : end + 1])
            keys.append(key)

        listed = client.list_objects_v2(Bucket=bucket, Prefix="encoded/")
        self.assertEqual(len(listed.get("Contents", [])), len(keys))
        for key in keys:
            client.delete_object(Bucket=bucket, Key=key)
        client.delete_bucket(Bucket=bucket)
