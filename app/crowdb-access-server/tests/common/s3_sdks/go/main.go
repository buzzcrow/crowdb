// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
)

type observer struct{ checksum atomic.Bool }

func (o *observer) RoundTrip(r *http.Request) (*http.Response, error) {
	if r.Method == "PUT" && strings.HasSuffix(r.URL.Path, "/prefix/ordinary") {
		if r.Header.Get("x-amz-checksum-crc32") != "" || strings.Contains(r.Header.Get("x-amz-trailer"), "crc32") {
			o.checksum.Store(true)
		}
	}
	return http.DefaultTransport.RoundTrip(r)
}

type suite struct {
	ctx       context.Context
	client    *s3.Client
	bucket    *string
	created   bool
	current   string
	transport *observer
}

func env(name string) string {
	value := os.Getenv(name)
	check(value != "", "missing "+name)
	return value
}

func (s *suite) newClient(invalid bool) *s3.Client {
	access, secret := env("CROWDB_S3_E2E_ACCESS_KEY"), env("CROWDB_S3_E2E_SECRET_KEY")
	if invalid {
		access, secret = "invalid", "invalid"
	}
	cfg := must(config.LoadDefaultConfig(s.ctx,
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(access, secret, "")),
		config.WithHTTPClient(&http.Client{Transport: s.transport, Timeout: 60 * time.Second})))
	return s3.NewFromConfig(cfg, func(options *s3.Options) {
		options.BaseEndpoint = aws.String(env("CROWDB_S3_E2E_ENDPOINT"))
		options.UsePathStyle = true
	})
}

func must[T any](value T, err error) T {
	if err != nil {
		panic(err)
	}
	return value
}

type assertionFailure string

func check(ok bool, message string) {
	if !ok {
		panic(assertionFailure(message))
	}
}

func expect(code string, err error) {
	var api smithy.APIError
	check(errors.As(err, &api) && api.ErrorCode() == code, "unexpected S3 error")
}

func (s *suite) step(name string) { s.current = name; fmt.Println("go: " + name) }

func (s *suite) cleanup() {
	if !s.created {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	var keyMarker, uploadMarker *string
	for {
		page := must(s.client.ListMultipartUploads(ctx, &s3.ListMultipartUploadsInput{Bucket: s.bucket, KeyMarker: keyMarker, UploadIdMarker: uploadMarker}))
		for _, upload := range page.Uploads {
			must(s.client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{Bucket: s.bucket, Key: upload.Key, UploadId: upload.UploadId}))
		}
		if !aws.ToBool(page.IsTruncated) {
			break
		}
		keyMarker, uploadMarker = page.NextKeyMarker, page.NextUploadIdMarker
	}
	var keys []*string
	pages := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{Bucket: s.bucket})
	for pages.HasMorePages() {
		for _, object := range must(pages.NextPage(ctx)).Contents {
			keys = append(keys, object.Key)
		}
	}
	for _, key := range keys {
		must(s.client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: s.bucket, Key: key}))
	}
	must(s.client.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: s.bucket}))
	for _, bucket := range must(s.client.ListBuckets(ctx, &s3.ListBucketsInput{})).Buckets {
		check(aws.ToString(bucket.Name) != *s.bucket, "cleanup bucket still exists")
	}
	fmt.Println("go: owned bucket and multipart sessions cleaned")
}

type injectedFailure struct{}

func capture(action func()) (failure any) {
	defer func() { failure = recover() }()
	action()
	return nil
}

func run() int {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	id := make([]byte, 16)
	must(rand.Read(id))
	s := &suite{ctx: ctx, bucket: aws.String("crowdb-go-" + hex.EncodeToString(id)), current: "setup", transport: &observer{}}
	failure := capture(func() {
		s.client = s.newClient(false)
		if os.Getenv("CROWDB_S3_SDK_FAULT") == "after-mpu" {
			s.failureScenario()
		}
		s.scenarios()
	})
	exit := 0
	if failure != nil {
		exit = 1
		if _, ok := failure.(injectedFailure); ok {
			exit = 42
		}
		// SDK errors can contain request URLs; report only the code/type and stage.
		code := fmt.Sprintf("%T", failure)
		if assertion, ok := failure.(assertionFailure); ok {
			code = "assertion: " + string(assertion)
		}
		if err, ok := failure.(error); ok {
			var api smithy.APIError
			if errors.As(err, &api) {
				code = api.ErrorCode()
			}
		}
		fmt.Fprintf(os.Stderr, "go: FAILED step=%s code=%s\n", s.current, code)
	}
	if capture(s.cleanup) != nil {
		fmt.Fprintln(os.Stderr, "go: cleanup FAILED")
		exit = 1
	}
	if exit == 0 {
		fmt.Println("go: PASS AWS SDK S3 1.114.0, net/http, default checksums/retries, sequential low-level MPU")
	}
	return exit
}

func main() { os.Exit(run()) }
