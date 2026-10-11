# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.

from base64 import b64encode
from hashlib import md5

from botocore.exceptions import ClientError


class BatchDeleteCases:
    def test_batch_delete_preserves_exact_keys_and_quiet(self):
        bucket = self.bucket + "-batch"
        keys = ["escaped/<&>.bin", "unicode/边界%25+.bin", "literal\rline.bin"]
        self.client.create_bucket(Bucket=bucket)
        for key in keys + ["retained"]:
            self.client.put_object(Bucket=bucket, Key=key, Body=b"selected")
        selected = keys + [keys[0], "absent"]
        result = self.client.delete_objects(
            Bucket=bucket, Delete={"Objects": [{"Key": key} for key in selected]})
        self.assertEqual([item["Key"] for item in result["Deleted"]], selected)
        self.assertNotIn("Errors", result)
        self.assertEqual([item["Key"] for item in self.client.list_objects_v2(
            Bucket=bucket)["Contents"]], ["retained"])
        self.assertEqual(self.client.get_object(Bucket=bucket, Key="retained")["Body"].read(), b"selected")
        quiet = self.client.delete_objects(Bucket=bucket, Delete={
            "Objects": [{"Key": key} for key in selected + ["retained"]], "Quiet": True})
        self.assertNotIn("Deleted", quiet)
        self.assertNotIn("Errors", quiet)
        self.client.delete_bucket(Bucket=bucket)

    def test_batch_delete_thousand_keys_and_unversioned_retry(self):
        bucket = self.bucket + "-batch-limit"
        keys = [f"bulk/{index:04}/<&>边界" for index in range(1000)]
        self.client.create_bucket(Bucket=bucket)
        # Existing and absent keys share the same idempotent success contract.
        for key in keys[:8]:
            self.client.put_object(Bucket=bucket, Key=key, Body=b"old")
        selection = {"Objects": [{"Key": key} for key in keys]}
        for _ in range(2):
            result = self.client.delete_objects(Bucket=bucket, Delete=selection)
            self.assertEqual([item["Key"] for item in result["Deleted"]], keys)
            self.assertNotIn("Errors", result)
        self.client.put_object(Bucket=bucket, Key=keys[0], Body=b"new publication")
        self.client.delete_objects(Bucket=bucket, Delete=selection)
        self.assertEqual(self.client.list_objects_v2(Bucket=bucket)["KeyCount"], 0)
        self.client.delete_bucket(Bucket=bucket)

    def test_batch_delete_rejects_entire_invalid_request(self):
        bucket = self.bucket + "-batch-invalid"
        self.client.create_bucket(Bucket=bucket)
        self.client.put_object(Bucket=bucket, Key="retained", Body=b"unchanged")
        valid = b"<Delete><Object><Key>retained</Key></Object></Delete>"
        bodies = [
            b"<Delete><Object><Key>retained</Key></Object><Bad/></Delete>",
            b"<Delete><Object><Key>retained</Key><VersionId>v</VersionId></Object></Delete>",
            b"<Delete>" + b"<Object><Key>retained</Key></Object>" * 1001 + b"</Delete>",
            b"<Delete><Object><Key>retained</Key></Object>",
            b"<!DOCTYPE Delete [<!ENTITY key 'retained'>]><Delete><Object><Key>&key;</Key></Object></Delete>",
            b" " * (2 * 1024 * 1024 + 1),
        ]
        requests = [(body, {"Content-MD5": b64encode(md5(body).digest()).decode()}) for body in bodies]
        requests += [(valid, {}), (valid, {"Content-MD5": "bad"}),
                     (valid, {"Content-MD5": b64encode(bytes(16)).decode()}),
                     (valid, {"x-amz-checksum-crc32": b64encode(bytes(4)).decode()})]
        for body, headers in requests:
            status, _, response = self.signed_http("POST", f"/{bucket}?delete", body, headers)
            self.assertIn(status, [400, 501], response)
            self.assertEqual(self.client.get_object(Bucket=bucket, Key="retained")["Body"].read(), b"unchanged")
        headers = {"Content-MD5": b64encode(md5(valid).digest()).decode()}
        status, _, _ = self.signed_http("POST", f"/{bucket}?delete", valid, headers, corrupt_signature=True)
        self.assertEqual(status, 403)
        with self.assertRaises(ClientError) as missing:
            self.client.delete_objects(Bucket=bucket + "-missing", Delete={"Objects": [{"Key": "retained"}]})
        self.assertEqual(missing.exception.response["Error"]["Code"], "NoSuchBucket")
        self.assertEqual(self.client.get_object(Bucket=bucket, Key="retained")["Body"].read(), b"unchanged")
        self.client.delete_object(Bucket=bucket, Key="retained")
        self.client.delete_bucket(Bucket=bucket)
