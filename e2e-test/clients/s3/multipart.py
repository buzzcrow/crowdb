# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.

from base import *


class MultipartCases:
    def test_multipart_replaces_parts_and_publishes_selected_bytes(self):
        bucket = f"{self.bucket}-multipart"
        key = "parts/object.bin"
        first = b"a" * (5 * 1024 * 1024)
        replacement = b"b" * len(first)
        tail = b"final-part"
        self.client.create_bucket(Bucket=bucket)
        try:
            upload_id = self.client.create_multipart_upload(Bucket=bucket, Key=key)["UploadId"]
            self.assertIn(upload_id, [item["UploadId"] for item in
                                  self.client.list_multipart_uploads(Bucket=bucket)["Uploads"]])
            tail_etag = self.client.upload_part(
                Bucket=bucket, Key=key, UploadId=upload_id, PartNumber=2, Body=tail,
            )["ETag"]
            self.assertEqual(self.client.upload_part(
                Bucket=bucket, Key=key, UploadId=upload_id, PartNumber=2, Body=tail,
            )["ETag"], tail_etag)
            self.client.upload_part(Bucket=bucket, Key=key, UploadId=upload_id,
                                    PartNumber=1, Body=first)
            first_etag = self.client.upload_part(
                Bucket=bucket, Key=key, UploadId=upload_id, PartNumber=1, Body=replacement,
            )["ETag"]
            listed = self.client.list_parts(Bucket=bucket, Key=key, UploadId=upload_id)
            self.assertEqual([part["PartNumber"] for part in listed["Parts"]], [1, 2])
            self.assertEqual(listed["Parts"][0]["ETag"], first_etag)
            completed = self.client.complete_multipart_upload(
                Bucket=bucket, Key=key, UploadId=upload_id,
                MultipartUpload={"Parts": [
                    {"PartNumber": 1, "ETag": first_etag},
                    {"PartNumber": 2, "ETag": tail_etag},
                ]},
            )
            expected_etag = md5(md5(replacement).digest() + md5(tail).digest()).hexdigest() + "-2"
            self.assertEqual(completed["ETag"], f'"{expected_etag}"')
            replayed = self.client.complete_multipart_upload(
                Bucket=bucket, Key=key, UploadId=upload_id,
                MultipartUpload={"Parts": [
                    {"PartNumber": 1, "ETag": first_etag},
                    {"PartNumber": 2, "ETag": tail_etag},
                ]},
            )
            self.assertEqual(replayed["ETag"], completed["ETag"])
            self.assertEqual(self.client.get_object(Bucket=bucket, Key=key)["Body"].read(), replacement + tail)
            self.client.delete_object(Bucket=bucket, Key=key)

            aborted = self.client.create_multipart_upload(Bucket=bucket, Key=key)["UploadId"]
            self.client.upload_part(Bucket=bucket, Key=key, UploadId=aborted, PartNumber=1, Body=tail)
            self.client.abort_multipart_upload(Bucket=bucket, Key=key, UploadId=aborted)
            self.client.abort_multipart_upload(Bucket=bucket, Key=key, UploadId=aborted)
            self.assertNotIn(aborted, [item["UploadId"] for item in
                                       self.client.list_multipart_uploads(Bucket=bucket).get("Uploads", [])])

            invalid = self.client.create_multipart_upload(Bucket=bucket, Key=key)["UploadId"]
            self.client.upload_part(Bucket=bucket, Key=key, UploadId=invalid, PartNumber=1, Body=tail)
            with self.assertRaises(ClientError) as mismatch:
                self.client.complete_multipart_upload(
                    Bucket=bucket, Key=key, UploadId=invalid,
                    MultipartUpload={"Parts": [{"PartNumber": 1, "ETag": '"' + "00" * 16 + '"'}]},
                )
            self.assertEqual(mismatch.exception.response["Error"]["Code"], "InvalidPart")
            self.client.abort_multipart_upload(Bucket=bucket, Key=key, UploadId=invalid)
        finally:
            self.client.delete_bucket(Bucket=bucket)

    def test_multipart_copy_selects_ranges_and_replaces_parts(self):
        bucket = f"{self.bucket}-part-copy"
        payload = bytes(range(256)) * (24 * 1024)
        replacement = b"replacement" * (5 * 1024 * 1024 // 11 + 1)
        self.client.create_bucket(Bucket=bucket)
        self.client.put_object(Bucket=bucket, Key="source", Body=payload)
        self.client.put_object(Bucket=bucket, Key="replacement", Body=replacement)
        upload = self.client.create_multipart_upload(Bucket=bucket, Key="target")["UploadId"]
        options = dict(Bucket=bucket, Key="target", UploadId=upload, CopySource={"Bucket": bucket, "Key": "source"})
        first = self.client.upload_part_copy(**options, PartNumber=1,
                                            CopySourceRange="bytes=0-5242879")["CopyPartResult"]["ETag"]
        tail = self.client.upload_part_copy(**options, PartNumber=2,
                                           CopySourceRange=f"bytes=5242880-{len(payload)-1}")["CopyPartResult"]["ETag"]
        for invalid in ["bytes=4-3", "bytes=0-999999999", "bytes=0-", "bytes=-1"]:
            with self.assertRaises(ClientError) as error:
                self.client.upload_part_copy(**options, PartNumber=1, CopySourceRange=invalid)
            self.assertEqual(error.exception.response["Error"]["Code"], "InvalidRange")
            self.assertEqual(self.client.list_parts(Bucket=bucket, Key="target", UploadId=upload)["Parts"][0]["ETag"], first)
        first = self.client.upload_part_copy(Bucket=bucket, Key="target", UploadId=upload, PartNumber=1,
                                             CopySource={"Bucket": bucket, "Key": "replacement"})["CopyPartResult"]["ETag"]
        self.client.complete_multipart_upload(Bucket=bucket, Key="target", UploadId=upload,
                                              MultipartUpload={"Parts": [{"PartNumber": 1, "ETag": first}, {"PartNumber": 2, "ETag": tail}]})
        self.assertEqual(self.client.get_object(Bucket=bucket, Key="target")["Body"].read(), replacement + payload[5242880:])
        self.assertEqual(self.client.get_object(Bucket=bucket, Key="source")["Body"].read(), payload)
        with self.assertRaises(ClientError) as error:
            self.client.upload_part_copy(**options, PartNumber=1)
        self.assertEqual(error.exception.response["Error"]["Code"], "NoSuchUpload")
        for key in ["source", "replacement", "target"]:
            self.client.delete_object(Bucket=bucket, Key=key)
        self.client.delete_bucket(Bucket=bucket)
