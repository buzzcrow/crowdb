// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import software.amazon.awssdk.auth.credentials.*;
import software.amazon.awssdk.awscore.presigner.PresignedRequest;
import software.amazon.awssdk.core.interceptor.*;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.*;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.s3.presigner.model.*;

public final class TestS3 {
    private final String bucket = "crowdb-java-" + UUID.randomUUID();
    private final URI endpoint = URI.create(System.getenv("CROWDB_S3_E2E_ENDPOINT"));
    private final StaticCredentialsProvider credentials = StaticCredentialsProvider.create(
        AwsBasicCredentials.create(System.getenv("CROWDB_S3_E2E_ACCESS_KEY"), System.getenv("CROWDB_S3_E2E_SECRET_KEY")));
    private final AtomicBoolean checksum = new AtomicBoolean();
    private final AtomicBoolean cleaned = new AtomicBoolean();
    private final S3Client client = client(credentials, true);
    private boolean created;
    private String step = "setup";

    private S3Client client(AwsCredentialsProvider provider, boolean observe) {
        return S3Client.builder().endpointOverride(endpoint).region(Region.US_EAST_1)
            .credentialsProvider(provider).forcePathStyle(true)
            .httpClientBuilder(UrlConnectionHttpClient.builder().connectionTimeout(Duration.ofSeconds(10))
                .socketTimeout(Duration.ofSeconds(30)))
            .overrideConfiguration(c -> {
                c.apiCallTimeout(Duration.ofSeconds(60));
                if (observe) c.addExecutionInterceptor(new ExecutionInterceptor() {
                    @Override public void beforeTransmission(Context.BeforeTransmission ctx, ExecutionAttributes attrs) {
                        if (ctx.request() instanceof PutObjectRequest r && r.key().equals("prefix/ordinary")) {
                            var h = ctx.httpRequest();
                            if (h.firstMatchingHeader("x-amz-checksum-crc32").isPresent()
                                || h.firstMatchingHeader("x-amz-trailer").orElse("").contains("crc32")) checksum.set(true);
                        }
                    }
                });
            }).build();
    }

    private void step(String name) { step = name; System.out.println("java: " + name); }
    private static void check(boolean value, String message) {
        if (!value) throw new AssertionError(message);
    }
    private static void expect(String code, Runnable action) {
        try { action.run(); } catch (S3Exception e) {
            check(code.equals(e.awsErrorDetails().errorCode()), "unexpected S3 error code"); return;
        }
        throw new AssertionError("expected " + code);
    }
    private byte[] get(String key) {
        return client.getObjectAsBytes(r -> r.bucket(bucket).key(key)).asByteArray();
    }
    private void put(String key, byte[] bytes) {
        client.putObject(r -> r.bucket(bucket).key(key), RequestBody.fromBytes(bytes));
    }

    private void scenarios() throws Exception {
        byte[] small = "SDK exact bytes".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        Map<String, String> metadata = Map.of("mtime", "1700000000.123", "origin", "java");
        step("bucket discovery and ordinary default checksum");
        client.createBucket(r -> r.bucket(bucket)); created = true;
        client.headBucket(r -> r.bucket(bucket));
        check(client.listBuckets().buckets().stream().anyMatch(b -> b.name().equals(bucket)), "bucket discovery");
        client.putObject(r -> r.bucket(bucket).key("prefix/ordinary").metadata(metadata), RequestBody.fromBytes(small));
        check(checksum.get(), "default CRC32 was not transmitted");
        check(Arrays.equals(get("prefix/ordinary"), small), "ordinary bytes");
        check(client.headObject(r -> r.bucket(bucket).key("prefix/ordinary")).metadata().equals(metadata), "HEAD metadata");
        var range = client.getObjectAsBytes(r -> r.bucket(bucket).key("prefix/ordinary").range("bytes=1-4"));
        check(Arrays.equals(range.asByteArray(), Arrays.copyOfRange(small, 1, 5)), "range bytes");
        check(range.response().metadata().equals(metadata), "GET metadata");

        step("copy, replacement metadata and paginated prefix listing");
        client.copyObject(r -> r.bucket(bucket).key("copied").copySource(bucket + "/prefix/ordinary"));
        check(Arrays.equals(get("copied"), small), "copy bytes");
        check(client.headObject(r -> r.bucket(bucket).key("copied")).metadata().equals(metadata), "copy metadata");
        client.copyObject(r -> r.bucket(bucket).key("copied").copySource(bucket + "/copied")
            .metadataDirective(MetadataDirective.REPLACE).metadata(Map.of("origin", "replacement")));
        check(client.headObject(r -> r.bucket(bucket).key("copied")).metadata().equals(Map.of("origin", "replacement")), "replace metadata");
        for (String key : List.of("prefix/a", "prefix/b", "outside")) put(key, small);
        Set<String> keys = new HashSet<>(); int pages = 0;
        for (var page : client.listObjectsV2Paginator(r -> r.bucket(bucket).prefix("prefix/").maxKeys(1))) {
            pages++;
            check(page.contents().size() <= 1, "page exceeds requested limit");
            for (var item : page.contents()) check(keys.add(item.key()), "duplicate listed key");
        }
        check(pages >= 3 && keys.equals(Set.of("prefix/a", "prefix/b", "prefix/ordinary")), "prefix pagination");

        step("low-level multipart and abort");
        byte[] first = new byte[5 * 1024 * 1024];
        for (int i = 0; i < first.length; i++) first[i] = (byte)(i % 251);
        String upload = client.createMultipartUpload(r -> r.bucket(bucket).key("multipart").metadata(metadata)).uploadId();
        var one = client.uploadPart(r -> r.bucket(bucket).key("multipart").uploadId(upload).partNumber(1), RequestBody.fromBytes(first));
        var two = client.uploadPart(r -> r.bucket(bucket).key("multipart").uploadId(upload).partNumber(2), RequestBody.fromBytes(small));
        check(client.listParts(r -> r.bucket(bucket).key("multipart").uploadId(upload)).parts().size() == 2, "list parts");
        client.completeMultipartUpload(r -> r.bucket(bucket).key("multipart").uploadId(upload)
            .multipartUpload(m -> m.parts(CompletedPart.builder().partNumber(1).eTag(one.eTag()).build(),
                                         CompletedPart.builder().partNumber(2).eTag(two.eTag()).build())));
        byte[] expected = Arrays.copyOf(first, first.length + small.length);
        System.arraycopy(small, 0, expected, first.length, small.length);
        check(Arrays.equals(get("multipart"), expected), "multipart bytes");
        check(client.headObject(r -> r.bucket(bucket).key("multipart")).metadata().equals(metadata), "multipart metadata");
        String abort = client.createMultipartUpload(r -> r.bucket(bucket).key("abandoned")).uploadId();
        client.uploadPart(r -> r.bucket(bucket).key("abandoned").uploadId(abort).partNumber(1), RequestBody.fromBytes(small));
        client.abortMultipartUpload(r -> r.bucket(bucket).key("abandoned").uploadId(abort));
        expect("NoSuchKey", () -> get("abandoned"));

        step("SDK presigned PUT and GET");
        try (var presigner = S3Presigner.builder().endpointOverride(endpoint).region(Region.US_EAST_1)
            .credentialsProvider(credentials).serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build()).build()) {
            var put = presigner.presignPutObject(r -> r.signatureDuration(Duration.ofMinutes(2))
                .putObjectRequest(p -> p.bucket(bucket).key("presigned")));
            check(http(put, "PUT", small).statusCode() == 200, "presigned PUT");
            var get = presigner.presignGetObject(r -> r.signatureDuration(Duration.ofMinutes(2))
                .getObjectRequest(p -> p.bucket(bucket).key("presigned")));
            var response = http(get, "GET", null);
            check(response.statusCode() == 200 && Arrays.equals(response.body(), small), "presigned GET");
        }

        step("missing object, invalid credentials and corrupt checksum preservation");
        expect("NoSuchKey", () -> get("missing"));
        expect("BadDigest", () -> client.putObject(r -> r.bucket(bucket).key("prefix/ordinary").checksumCRC32("AAAAAA=="), RequestBody.fromBytes(small)));
        check(Arrays.equals(get("prefix/ordinary"), small), "failed PUT changed object");
        try (var invalid = client(StaticCredentialsProvider.create(AwsBasicCredentials.create("invalid", "invalid")), false)) {
            try { invalid.listBuckets(); throw new AssertionError("invalid credentials accepted"); }
            catch (S3Exception e) { check(e.statusCode() == 403, "invalid credential status"); }
        }
        step("single and batch deletion");
        client.deleteObject(r -> r.bucket(bucket).key("outside"));
        expect("NoSuchKey", () -> get("outside"));
        var deleted = client.deleteObjects(r -> r.bucket(bucket).delete(d -> d.objects(
            ObjectIdentifier.builder().key("prefix/a").build(), ObjectIdentifier.builder().key("prefix/b").build())));
        check(deleted.errors().isEmpty() && deleted.deleted().size() == 2, "batch deletion");
        expect("NoSuchKey", () -> get("prefix/a"));
        expect("NoSuchKey", () -> get("prefix/b"));
    }

    private static HttpResponse<byte[]> http(PresignedRequest signed, String method, byte[] body) throws Exception {
        var request = HttpRequest.newBuilder(signed.url().toURI()).timeout(Duration.ofSeconds(30));
        signed.signedHeaders().forEach((key, values) -> {
            if (!key.equalsIgnoreCase("host") && !key.equalsIgnoreCase("content-length"))
                values.forEach(value -> request.header(key, value));
        });
        request.method(method, body == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofByteArray(body));
        try (var http = HttpClient.newHttpClient()) { return http.send(request.build(), HttpResponse.BodyHandlers.ofByteArray()); }
    }

    private void cleanup() {
        if (!created || !cleaned.compareAndSet(false, true)) return;
        for (var page : client.listMultipartUploadsPaginator(r -> r.bucket(bucket)))
            for (var upload : page.uploads()) client.abortMultipartUpload(r -> r.bucket(bucket).key(upload.key()).uploadId(upload.uploadId()));
        // Collect before deletion so pagination markers never depend on deleted entries.
        var keys = new ArrayList<String>();
        for (var page : client.listObjectsV2Paginator(r -> r.bucket(bucket)))
            for (var object : page.contents()) keys.add(object.key());
        for (var key : keys) client.deleteObject(r -> r.bucket(bucket).key(key));
        client.deleteBucket(r -> r.bucket(bucket));
        check(client.listBuckets().buckets().stream().noneMatch(b -> b.name().equals(bucket)), "cleanup bucket still exists");
        System.out.println("java: owned bucket and multipart sessions cleaned");
    }

    private static final class InjectedFailure extends RuntimeException {}

    private void failureScenario() {
        step("injected failure after an uploaded part");
        client.createBucket(r -> r.bucket(bucket)); created = true;
        String upload = client.createMultipartUpload(r -> r.bucket(bucket).key("unfinished")).uploadId();
        client.uploadPart(r -> r.bucket(bucket).key("unfinished").uploadId(upload).partNumber(1), RequestBody.fromString("unfinished bytes"));
        throw new InjectedFailure();
    }

    public static void main(String[] args) {
        TestS3 test = new TestS3();
        var hook = new Thread(() -> { try { test.cleanup(); } catch (Throwable e) { System.err.println("java: interrupt cleanup failed"); } });
        Runtime.getRuntime().addShutdownHook(hook);
        int exit = 1;
        try {
            if ("after-mpu".equals(System.getenv("CROWDB_S3_SDK_FAULT"))) test.failureScenario();
            test.scenarios(); exit = 0;
        }
        catch (Throwable e) {
            if (e instanceof InjectedFailure) exit = 42;
            String code = e instanceof S3Exception s ? s.awsErrorDetails().errorCode() : e.getClass().getSimpleName();
            System.err.println("java: FAILED step=" + test.step + " code=" + code);
            if (e instanceof AssertionError) System.err.println("java: assertion=" + e.getMessage());
        } finally {
            try { test.cleanup(); } catch (Throwable e) { exit = 1; System.err.println("java: cleanup FAILED"); }
            Runtime.getRuntime().removeShutdownHook(hook); test.client.close();
        }
        if (exit != 0) System.exit(exit);
        System.out.println("java: PASS AWS SDK 2.55.10, URLConnection, default checksums/retries, sequential low-level MPU");
    }
}
