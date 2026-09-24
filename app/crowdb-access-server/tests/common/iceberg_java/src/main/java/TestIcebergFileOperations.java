import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.Tag;

public final class TestIcebergFileOperations {
  public static void run(S3Client client, String prefix) {
    URI location = URI.create(prefix);
    String bucket = location.getHost();
    String root = location.getPath().substring(1);
    String key = root + "metadata/operations.json";
    byte[] bytes = "{\"immutable\":true}".getBytes(StandardCharsets.UTF_8);
    client.putObject(request -> request.bucket(bucket).key(key), RequestBody.fromBytes(bytes));
    String etag = client.headObject(request -> request.bucket(bucket).key(key)).eTag();
    client.putObject(request -> request.bucket(bucket).key(key), RequestBody.fromBytes(bytes));
    reject(409, "OperationAborted", () -> client.putObject(request -> request.bucket(bucket).key(key),
        RequestBody.fromString("{\"immutable\":false}")));
    reject(400, "InvalidRequest", () -> client.createBucket(request -> request.bucket(bucket)));
    reject(400, "InvalidRequest", () -> client.deleteBucket(request -> request.bucket(bucket)));
    reject(400, "InvalidRequest", () -> client.listObjectsV2(request -> request.bucket(bucket)));
    reject(400, "InvalidRequest", () -> client.getBucketLifecycleConfiguration(request -> request.bucket(bucket)));
    reject(400, "InvalidRequest", () -> client.deleteBucketLifecycle(request -> request.bucket(bucket)));
    reject(400, "InvalidRequest", () -> client.getObjectTagging(request -> request.bucket(bucket).key(key)));
    reject(400, "InvalidRequest", () -> client.putObjectTagging(request -> request.bucket(bucket).key(key)
        .tagging(tags -> tags.tagSet(Tag.builder().key("owner").value("changed").build()))));
    reject(400, "InvalidRequest", () -> client.deleteObjectTagging(request -> request.bucket(bucket).key(key)));
    reject(400, "InvalidRequest", () -> client.deleteObject(request -> request.bucket(bucket).key(key)));
    reject(400, "InvalidRequest", () -> client.putObject(request -> request.bucket(bucket)
        .key(root + "../escape.json"), RequestBody.fromBytes(bytes)));
    String foreign = "t/" + (root.charAt(2) == '0' ? '1' : '0') + root.substring(3) + "metadata/foreign.json";
    reject(403, "AccessDenied", () -> client.putObject(request -> request.bucket(bucket).key(foreign),
        RequestBody.fromBytes(bytes)));
    if (!etag.equals(client.headObject(request -> request.bucket(bucket).key(key)).eTag())
        || !Arrays.equals(bytes, client.getObjectAsBytes(request -> request.bucket(bucket).key(key)).asByteArray())) {
      throw new AssertionError("unsupported operations changed immutable authority");
    }
    System.out.println("Official S3 operation restrictions, immutable replay and exact table scope passed");
  }

  private static void reject(int status, String code, Runnable operation) {
    try {
      operation.run();
      throw new AssertionError("unsupported operation succeeded");
    } catch (S3Exception failure) {
      if (failure.statusCode() != status || !code.equals(failure.awsErrorDetails().errorCode())) {
        throw failure;
      }
    }
  }
}
