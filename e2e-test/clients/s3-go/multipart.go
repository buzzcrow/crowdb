// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

package main

import (
	"bytes"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

func (s *suite) multipart(body []byte, metadata map[string]string) {
	s.step("low-level multipart and abort")
	first := make([]byte, 5*1024*1024)
	for i := range first {
		first[i] = byte(i % 251)
	}
	key := aws.String("multipart")
	upload := must(s.client.CreateMultipartUpload(s.ctx, &s3.CreateMultipartUploadInput{Bucket: s.bucket, Key: key, Metadata: metadata})).UploadId
	var parts []types.CompletedPart
	for i, data := range [][]byte{first, body} {
		number := aws.Int32(int32(i + 1))
		part := must(s.client.UploadPart(s.ctx, &s3.UploadPartInput{Bucket: s.bucket, Key: key, UploadId: upload, PartNumber: number, Body: bytes.NewReader(data)}))
		parts = append(parts, types.CompletedPart{PartNumber: number, ETag: part.ETag})
	}
	check(len(must(s.client.ListParts(s.ctx, &s3.ListPartsInput{Bucket: s.bucket, Key: key, UploadId: upload})).Parts) == 2, "list parts")
	must(s.client.CompleteMultipartUpload(s.ctx, &s3.CompleteMultipartUploadInput{Bucket: s.bucket, Key: key, UploadId: upload, MultipartUpload: &types.CompletedMultipartUpload{Parts: parts}}))
	check(bytes.Equal(s.get("multipart"), append(first, body...)), "multipart bytes")
	s.head("multipart", metadata)
	abortKey := aws.String("abandoned")
	abort := must(s.client.CreateMultipartUpload(s.ctx, &s3.CreateMultipartUploadInput{Bucket: s.bucket, Key: abortKey})).UploadId
	must(s.client.UploadPart(s.ctx, &s3.UploadPartInput{Bucket: s.bucket, Key: abortKey, UploadId: abort, PartNumber: aws.Int32(1), Body: bytes.NewReader(body)}))
	must(s.client.AbortMultipartUpload(s.ctx, &s3.AbortMultipartUploadInput{Bucket: s.bucket, Key: abortKey, UploadId: abort}))
	s.missing("abandoned")
}

func (s *suite) failureScenario() {
	s.step("injected failure after an uploaded part")
	must(s.client.CreateBucket(s.ctx, &s3.CreateBucketInput{Bucket: s.bucket}))
	s.created = true
	key := aws.String("unfinished")
	upload := must(s.client.CreateMultipartUpload(s.ctx, &s3.CreateMultipartUploadInput{Bucket: s.bucket, Key: key})).UploadId
	must(s.client.UploadPart(s.ctx, &s3.UploadPartInput{Bucket: s.bucket, Key: key, UploadId: upload, PartNumber: aws.Int32(1), Body: bytes.NewReader([]byte("unfinished bytes"))}))
	panic(injectedFailure{})
}
