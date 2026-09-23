import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.io.SeekableInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.S3Exception;

public class TestIcebergFileIO {
  public static void main(String[] args) throws Exception {
    Properties configuration = new Properties();
    configuration.load(System.in);
    Map<String, String> properties = new HashMap<>();
    properties.put("s3.endpoint", configuration.getProperty("endpoint"));
    properties.put("client.region", "us-east-1");
    properties.put("s3.path-style-access", "true");
    properties.put("s3.multipart.part-size-bytes", "5242880");
    properties.put("s3.multipart.threshold", "1.0");
    AtomicInteger credentialRequests = new AtomicInteger();
    HttpServer credentials = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    byte[] credentialResponse = Base64.getDecoder().decode(configuration.getProperty("credentials"));
    credentials.createContext("/v1/namespaces/test/tables/test/credentials", exchange -> {
      if (!"GET".equals(exchange.getRequestMethod())) {
        exchange.sendResponseHeaders(405, -1);
        exchange.close();
        return;
      }
      credentialRequests.incrementAndGet();
      exchange.getResponseHeaders().set("Content-Type", "application/json");
      exchange.sendResponseHeaders(200, credentialResponse.length);
      try (var output = exchange.getResponseBody()) {
        output.write(credentialResponse);
      }
    });
    credentials.start();
    properties.put("uri", "http://127.0.0.1:" + credentials.getAddress().getPort());
    properties.put("client.refresh-credentials-endpoint", "/v1/namespaces/test/tables/test/credentials");
    properties.put("rest.auth.type", "none");
    try (S3FileIO files = new S3FileIO()) {
      files.initialize(properties);
      String prefix = configuration.getProperty("location");
      byte[] small = "{\"client\":\"iceberg-java-1.11.0\"}".getBytes(StandardCharsets.UTF_8);
      verify(files, prefix + "metadata/sdk-small.json", small);
      byte[] large = new byte[6 * 1024 * 1024];
      Arrays.fill(large, (byte) 'x');
      byte[] start = "{\"data\":\"".getBytes(StandardCharsets.UTF_8);
      System.arraycopy(start, 0, large, 0, start.length);
      large[large.length - 2] = '"';
      large[large.length - 1] = '}';
      verify(files, prefix + "metadata/sdk-multipart.json", large);
      verifyLateError(files.client(), prefix + "metadata/sdk-invalid.json");
      if (credentialRequests.get() != 1) {
        throw new AssertionError("SDK did not fetch and cache the delegated credential response");
      }
    } finally {
      credentials.stop(0);
    }
    System.out.println("Apache Iceberg 1.11.0 S3FileIO PUT, multipart, HEAD, GET, seek and embedded error passed");
  }

  private static void verifyLateError(S3Client client, String location) {
    URI uri = URI.create(location);
    String bucket = uri.getHost();
    String key = uri.getPath().substring(1);
    String upload = client.createMultipartUpload(request -> request.bucket(bucket).key(key)).uploadId();
    String etag = client.uploadPart(
        request -> request.bucket(bucket).key(key).uploadId(upload).partNumber(1),
        RequestBody.fromString("{invalid-json")).eTag();
    try {
      client.completeMultipartUpload(request -> request.bucket(bucket).key(key).uploadId(upload)
          .multipartUpload(parts -> parts.parts(CompletedPart.builder().partNumber(1).eTag(etag).build())));
      throw new AssertionError("SDK accepted an embedded Complete error as success");
    } catch (S3Exception error) {
      if (!"InvalidRequest".equals(error.awsErrorDetails().errorCode())) {
        throw error;
      }
    } finally {
      client.abortMultipartUpload(request -> request.bucket(bucket).key(key).uploadId(upload));
    }
  }

  private static void verify(S3FileIO files, String location, byte[] bytes) throws Exception {
    try (PositionOutputStream output = files.newOutputFile(location).create()) {
      output.write(bytes);
    }
    InputFile file = files.newInputFile(location);
    if (!file.exists() || file.getLength() != bytes.length) {
      throw new AssertionError("HEAD returned incorrect file state");
    }
    try (SeekableInputStream input = file.newStream()) {
      if (!Arrays.equals(input.readAllBytes(), bytes)) {
        throw new AssertionError("GET changed canonical bytes");
      }
      input.seek(bytes.length - 2L);
      if (input.read() != bytes[bytes.length - 2] || input.read() != bytes[bytes.length - 1]) {
        throw new AssertionError("Range GET returned incorrect tail");
      }
    }
  }
}
