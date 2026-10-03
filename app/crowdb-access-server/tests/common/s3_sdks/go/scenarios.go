// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

package main

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"reflect"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

func (s *suite) get(key string) []byte {
	result := must(s.client.GetObject(s.ctx, &s3.GetObjectInput{Bucket: s.bucket, Key: aws.String(key)}))
	defer result.Body.Close()
	return must(io.ReadAll(result.Body))
}

func (s *suite) missing(key string) {
	_, err := s.client.GetObject(s.ctx, &s3.GetObjectInput{Bucket: s.bucket, Key: aws.String(key)})
	expect("NoSuchKey", err)
}

func (s *suite) put(key string, body []byte) {
	must(s.client.PutObject(s.ctx, &s3.PutObjectInput{Bucket: s.bucket, Key: aws.String(key), Body: bytes.NewReader(body)}))
}

func (s *suite) head(key string, metadata map[string]string) {
	result := must(s.client.HeadObject(s.ctx, &s3.HeadObjectInput{Bucket: s.bucket, Key: aws.String(key)}))
	check(reflect.DeepEqual(result.Metadata, metadata), "HEAD metadata")
}

func (s *suite) scenarios() {
	body := []byte("SDK exact bytes")
	metadata := map[string]string{"mtime": "1700000000.123", "origin": "go"}
	s.step("bucket discovery and ordinary default checksum")
	must(s.client.CreateBucket(s.ctx, &s3.CreateBucketInput{Bucket: s.bucket}))
	s.created = true
	must(s.client.HeadBucket(s.ctx, &s3.HeadBucketInput{Bucket: s.bucket}))
	found := false
	for _, bucket := range must(s.client.ListBuckets(s.ctx, &s3.ListBucketsInput{})).Buckets {
		found = found || aws.ToString(bucket.Name) == *s.bucket
	}
	check(found, "bucket discovery")
	must(s.client.PutObject(s.ctx, &s3.PutObjectInput{Bucket: s.bucket, Key: aws.String("prefix/ordinary"), Body: bytes.NewReader(body), Metadata: metadata}))
	check(s.transport.checksum.Load(), "default CRC32 was not transmitted")
	check(bytes.Equal(s.get("prefix/ordinary"), body), "ordinary bytes")
	s.head("prefix/ordinary", metadata)
	ranged := must(s.client.GetObject(s.ctx, &s3.GetObjectInput{Bucket: s.bucket, Key: aws.String("prefix/ordinary"), Range: aws.String("bytes=1-4")}))
	actual := must(io.ReadAll(ranged.Body))
	must(struct{}{}, ranged.Body.Close())
	check(bytes.Equal(actual, body[1:5]) && reflect.DeepEqual(ranged.Metadata, metadata), "range bytes/metadata")
	s.copyAndList(body, metadata)
	s.multipart(body, metadata)
	s.presigned(body)
	s.negativeAndDelete(body)
}

func (s *suite) copyAndList(body []byte, metadata map[string]string) {
	s.step("copy, replacement metadata and paginated prefix listing")
	must(s.client.CopyObject(s.ctx, &s3.CopyObjectInput{Bucket: s.bucket, Key: aws.String("copied"), CopySource: aws.String(*s.bucket + "/prefix/ordinary")}))
	check(bytes.Equal(s.get("copied"), body), "copy bytes")
	s.head("copied", metadata)
	replacement := map[string]string{"origin": "replacement"}
	must(s.client.CopyObject(s.ctx, &s3.CopyObjectInput{Bucket: s.bucket, Key: aws.String("copied"), CopySource: aws.String(*s.bucket + "/copied"), MetadataDirective: types.MetadataDirectiveReplace, Metadata: replacement}))
	s.head("copied", replacement)
	for _, key := range []string{"prefix/a", "prefix/b", "outside"} {
		s.put(key, body)
	}
	keys := map[string]bool{}
	pages := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{Bucket: s.bucket, Prefix: aws.String("prefix/"), MaxKeys: aws.Int32(1)})
	count := 0
	for pages.HasMorePages() {
		page := must(pages.NextPage(s.ctx))
		count++
		check(len(page.Contents) <= 1, "page exceeds limit")
		for _, item := range page.Contents {
			key := aws.ToString(item.Key)
			check(!keys[key], "duplicate key")
			keys[key] = true
		}
	}
	check(count >= 3 && reflect.DeepEqual(keys, map[string]bool{"prefix/a": true, "prefix/b": true, "prefix/ordinary": true}), "prefix pagination")
}

func (s *suite) presigned(body []byte) {
	s.step("SDK presigned PUT and GET")
	presigner := s3.NewPresignClient(s.client)
	put := must(presigner.PresignPutObject(s.ctx, &s3.PutObjectInput{Bucket: s.bucket, Key: aws.String("presigned")}, s3.WithPresignExpires(2*time.Minute)))
	request := must(http.NewRequestWithContext(s.ctx, put.Method, put.URL, bytes.NewReader(body)))
	request.Header = put.SignedHeader.Clone()
	client := &http.Client{Timeout: 30 * time.Second}
	response := must(client.Do(request))
	must(io.ReadAll(response.Body))
	response.Body.Close()
	check(response.StatusCode == 200, "presigned PUT")
	get := must(presigner.PresignGetObject(s.ctx, &s3.GetObjectInput{Bucket: s.bucket, Key: aws.String("presigned")}, s3.WithPresignExpires(2*time.Minute)))
	request = must(http.NewRequestWithContext(s.ctx, get.Method, get.URL, nil))
	request.Header = get.SignedHeader.Clone()
	response = must(client.Do(request))
	defer response.Body.Close()
	check(response.StatusCode == 200 && bytes.Equal(must(io.ReadAll(response.Body)), body), "presigned GET")
}

func (s *suite) negativeAndDelete(body []byte) {
	s.step("missing object, invalid credentials and corrupt checksum preservation")
	s.missing("missing")
	_, err := s.client.PutObject(s.ctx, &s3.PutObjectInput{Bucket: s.bucket, Key: aws.String("prefix/ordinary"), Body: bytes.NewReader(body), ChecksumCRC32: aws.String("AAAAAA==")})
	expect("BadDigest", err)
	check(bytes.Equal(s.get("prefix/ordinary"), body), "failed PUT changed object")
	_, err = s.newClient(true).ListBuckets(s.ctx, &s3.ListBucketsInput{})
	var response *smithyhttp.ResponseError
	check(errors.As(err, &response) && response.HTTPStatusCode() == 403, "invalid credentials")
	s.step("single and batch deletion")
	must(s.client.DeleteObject(s.ctx, &s3.DeleteObjectInput{Bucket: s.bucket, Key: aws.String("outside")}))
	s.missing("outside")
	deleted := must(s.client.DeleteObjects(s.ctx, &s3.DeleteObjectsInput{Bucket: s.bucket, Delete: &types.Delete{Objects: []types.ObjectIdentifier{{Key: aws.String("prefix/a")}, {Key: aws.String("prefix/b")}}}}))
	check(len(deleted.Errors) == 0 && len(deleted.Deleted) == 2, "batch deletion")
	s.missing("prefix/a")
	s.missing("prefix/b")
}
