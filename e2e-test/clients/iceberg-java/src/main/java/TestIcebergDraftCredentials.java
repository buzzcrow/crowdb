import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.aws.AwsClientProperties;
import org.apache.iceberg.aws.s3.VendedCredentialsProvider;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.types.Types;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;

public final class TestIcebergDraftCredentials {
  private static final ObjectMapper JSON = new ObjectMapper();
  private static final String TABLES = "/v1/namespaces/analytics/tables";
  private static final String CREDENTIALS = TABLES + "/events/credentials";
  private static final String TOKEN = "w".repeat(32);
  private static final AtomicInteger DRAFTS = new AtomicInteger();
  private static final AtomicInteger REFRESHES = new AtomicInteger();
  private static final AtomicBoolean EXPIRED = new AtomicBoolean();
  private static final Schema SCHEMA =
      new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));

  public static void main(String[] args) throws Exception {
    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext("/", TestIcebergDraftCredentials::handle);
    server.start();
    String endpoint = "http://127.0.0.1:" + server.getAddress().getPort();
    try (RESTCatalog catalog = new RESTCatalog()) {
      catalog.initialize("crowdb", Map.of(
          "uri", endpoint,
          "token", TOKEN,
          "io-impl", "org.apache.iceberg.aws.s3.S3FileIO",
          "client.region", "us-east-1",
          "rest-metrics-reporting-enabled", "false"));
      TableIdentifier name = TableIdentifier.of(Namespace.of("analytics"), "events");
      Transaction first = catalog.buildTable(name, SCHEMA).createTransaction();
      Transaction second = catalog.buildTable(name, SCHEMA).createTransaction();
      verify(first, endpoint, "1");
      verify(second, endpoint, "2");
      require(REFRESHES.get() == 2, "one refresh per exact draft");
      require(DRAFTS.get() == 2, "two invisible same-name drafts");
      EXPIRED.set(true);
      boolean rejected = false;
      try {
        verify(first, endpoint, "1");
      } catch (RuntimeException failure) {
        require(failure.getMessage().contains("Draft expired"), "precise expired draft failure");
        rejected = true;
      }
      require(rejected, "expired draft must not fall back to another same-name table");
      System.out.println("Official staged RESTCatalog credential refresh acceptance passed");
    } finally {
      server.stop(0);
    }
  }

  private static void verify(Transaction transaction, String endpoint, String draft) {
    Map<String, String> properties = new HashMap<>(transaction.table().io().properties());
    require(properties.get("client.refresh-credentials-endpoint").equals(CREDENTIALS + "?table-id=" + draft),
        "stage response config reaches S3FileIO unchanged");
    require(transaction.table().location().equals("s3://test-bucket/t/" + draft), "draft location");
    require(properties.get("uri").equals(endpoint), "catalog URI inherited by FileIO");
    require(properties.get("token").equals(TOKEN), "bearer inherited by FileIO");
    properties.put("s3.access-key-id", "expired");
    properties.put("s3.secret-access-key", "expired-secret");
    properties.put("s3.session-token", "expired-token");
    properties.put("s3.session-token-expires-at-ms", "1");
    try (VendedCredentialsProvider provider = (VendedCredentialsProvider)
        new AwsClientProperties(properties).credentialsProvider("expired", "expired-secret", "expired-token")) {
      AwsSessionCredentials credentials = (AwsSessionCredentials) provider.resolveCredentials();
      require(credentials.accessKeyId().equals("draft-" + draft), "exact draft access key");
      require(credentials.sessionToken().equals("session-" + draft), "exact draft session");
      require(provider.resolveCredentials().accessKeyId().equals("draft-" + draft), "cached grant");
    }
  }

  private static void handle(HttpExchange exchange) throws IOException {
    try {
      require(("Bearer " + TOKEN).equals(exchange.getRequestHeaders().getFirst("Authorization")),
          "bearer authentication on refresh and REST requests");
      String path = exchange.getRequestURI().getPath();
      if (path.equals("/v1/config")) {
        respond(exchange, Map.of("defaults", Map.of(), "overrides", Map.of()));
      } else if (path.equals(TABLES) && exchange.getRequestMethod().equals("POST")) {
        require(JSON.readTree(exchange.getRequestBody()).get("stage-create").asBoolean(), "staged create");
        String draft = Integer.toString(DRAFTS.incrementAndGet());
        TableMetadata metadata = TableMetadata.newTableMetadata(
            SCHEMA, PartitionSpec.unpartitioned(), "s3://test-bucket/t/" + draft, Map.of());
        respond(exchange, Map.of(
            "metadata", JSON.readTree(TableMetadataParser.toJson(metadata)),
            "config", Map.of("client.refresh-credentials-endpoint", CREDENTIALS + "?table-id=" + draft)));
      } else if (path.equals(CREDENTIALS) && exchange.getRequestMethod().equals("GET")) {
        String query = exchange.getRequestURI().getRawQuery();
        require(query.equals("table-id=1") || query.equals("table-id=2"), "exact refresh query");
        String draft = query.substring("table-id=".length());
        if (draft.equals("1") && EXPIRED.get()) {
          byte[] bytes = JSON.writeValueAsBytes(Map.of("error", Map.of(
              "code", 404, "type", "NoSuchTableException", "message", "Draft expired")));
          exchange.getResponseHeaders().set("Content-Type", "application/json");
          exchange.sendResponseHeaders(404, bytes.length);
          exchange.getResponseBody().write(bytes);
          return;
        }
        REFRESHES.incrementAndGet();
        respond(exchange, Map.of("storage-credentials", new Object[] {Map.of(
            "prefix", "s3://test-bucket/t/" + draft + "/",
            "config", Map.of(
                "s3.access-key-id", "draft-" + draft,
                "s3.secret-access-key", "secret-" + draft,
                "s3.session-token", "session-" + draft,
                "s3.session-token-expires-at-ms", Long.toString(Instant.now().plusSeconds(900).toEpochMilli())))}));
      } else {
        throw new IllegalStateException("Unexpected SDK request: " + exchange.getRequestURI());
      }
    } catch (Exception failure) {
      byte[] bytes = failure.toString().getBytes(StandardCharsets.UTF_8);
      exchange.sendResponseHeaders(400, bytes.length);
      exchange.getResponseBody().write(bytes);
    } finally {
      exchange.close();
    }
  }

  private static void respond(HttpExchange exchange, Object value) throws IOException {
    byte[] bytes = JSON.writeValueAsBytes(value);
    exchange.getResponseHeaders().set("Content-Type", "application/json");
    exchange.sendResponseHeaders(200, bytes.length);
    exchange.getResponseBody().write(bytes);
  }

  private static void require(boolean valid, String message) {
    if (!valid) {
      throw new IllegalStateException(message);
    }
  }
}
