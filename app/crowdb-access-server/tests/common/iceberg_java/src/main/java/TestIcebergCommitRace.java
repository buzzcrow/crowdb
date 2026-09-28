import java.util.List;
import java.util.Map;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.rest.ErrorHandler;
import org.apache.iceberg.rest.ErrorHandlers;
import org.apache.iceberg.rest.HTTPClient;
import org.apache.iceberg.rest.auth.AuthSession;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;

public final class TestIcebergCommitRace {
  private static final String PATH = "v1/namespaces/analytics/tables/events";

  public static void main(String[] args) throws Exception {
    try (HTTPClient root = HTTPClient.builder(Map.of()).uri(args[0])
        .withHeaders(Map.of("Authorization", "Bearer " + "w".repeat(32))).build();
        HTTPClient client = root.withAuthSession(AuthSession.EMPTY)) {
      UpdateTableRequest request = new UpdateTableRequest(List.of(),
          List.of(new MetadataUpdate.SetProperties(Map.of("loser-only", "never-visible"))));
      String first = conflict(client, request, args[1]);
      require(first.equals(conflict(client, request, args[1])), "exact durable conflict replay");
      conflict(client, new UpdateTableRequest(List.of(),
          List.of(new MetadataUpdate.SetProperties(Map.of("changed-input", "rejected")))), args[1]);
      LoadTableResponse loaded = client.get(PATH, LoadTableResponse.class,
          Map.of(), ErrorHandlers.tableErrorHandler());
      require("visible".equals(loaded.tableMetadata().properties().get("winner-only")),
          "winner selected");
      require(!loaded.tableMetadata().properties().containsKey("loser-only"), "loser never selected");
      require(!loaded.tableMetadata().properties().containsKey("changed-input"), "identity cannot rebind");
    }
    System.out.println("Official SDK head CAS conflict and durable replay passed");
  }

  private static String conflict(HTTPClient client, UpdateTableRequest request, String identity) {
    ErrorHandler official = (ErrorHandler) ErrorHandlers.tableCommitHandler();
    String[] body = {null};
    try {
      client.post(PATH, request, LoadTableResponse.class, Map.of("Idempotency-Key", identity),
          new ErrorHandler() {
            @Override
            public ErrorResponse parseResponse(int code, String json) {
              require(code == 409, "HTTP conflict status");
              body[0] = json;
              return official.parseResponse(code, json);
            }

            @Override
            public void accept(ErrorResponse error) {
              require(error.code() == 409, "wire conflict status");
              require("CommitFailedException".equals(error.type()), "wire conflict type");
              official.accept(error);
            }
          });
    } catch (CommitFailedException expected) {
      require(body[0] != null, "server-originated conflict");
      return body[0];
    }
    throw new AssertionError("CAS loser must fail, not rebase or succeed");
  }

  private static void require(boolean condition, String message) {
    if (!condition) {
      throw new AssertionError(message);
    }
  }
}
