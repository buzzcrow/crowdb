import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.rest.ErrorHandlers;
import org.apache.iceberg.rest.HTTPClient;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.rest.auth.AuthSession;
import org.apache.iceberg.rest.responses.ListNamespacesResponse;

public class TestIcebergNamespaces {
  public static void main(String[] args) throws Exception {
    Map<String, String> properties = new HashMap<>();
    properties.put("uri", args[0]);
    properties.put("token", "r".repeat(32));
    properties.put("rest-page-size", "1");
    properties.put("rest-metrics-reporting-enabled", "false");
    properties.put("io-impl", TestIcebergCatalogReads.TestNoFileIO.class.getName());
    try (HTTPClient root = HTTPClient.builder(properties).uri(args[0])
        .withHeaders(Map.of("Authorization", "Bearer " + "r".repeat(32))).build();
        HTTPClient client = root.withAuthSession(AuthSession.EMPTY)) {
      ListNamespacesResponse complete = client.get("v1/namespaces", Map.of(),
          ListNamespacesResponse.class, Map.of(), ErrorHandlers.namespaceErrorHandler());
      require(complete.nextPageToken() == null, "complete response has no continuation");
      require(complete.namespaces().equals(List.of(Namespace.of("analytics"))), "complete contents");
      ListNamespacesResponse first = client.get("v1/namespaces",
          Map.of("pageToken", "", "pageSize", "1"), ListNamespacesResponse.class,
          Map.of(), ErrorHandlers.namespaceErrorHandler());
      require(first.namespaces().isEmpty(), "stale first page is empty");
      require(first.nextPageToken() != null, "stale first page can continue");
    }
    try (RESTCatalog catalog = new RESTCatalog()) {
      catalog.initialize("crowdb", properties);
      require(catalog.listNamespaces().equals(List.of(Namespace.of("analytics"))),
          "official catalog follows empty pages through the live namespace and final stale page");
    }
    System.out.println("Java namespace pagination passed");
  }

  private static void require(boolean condition, String message) {
    if (!condition) {
      throw new AssertionError(message);
    }
  }
}
