import java.util.List;
import java.util.Map;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
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
      reject(client, request, args[1], 409);
      reject(client, request, args[1], 503);
      reject(client, new UpdateTableRequest(List.of(),
          List.of(new MetadataUpdate.SetProperties(Map.of("changed-input", "rejected")))), args[1], 503);
      LoadTableResponse loaded = client.get(PATH, LoadTableResponse.class,
          Map.of(), ErrorHandlers.tableErrorHandler());
      require("visible".equals(loaded.tableMetadata().properties().get("winner-only")),
          "winner selected");
      require(!loaded.tableMetadata().properties().containsKey("loser-only"), "loser never selected");
      require(!loaded.tableMetadata().properties().containsKey("changed-input"), "identity cannot rebind");
    }
    System.out.println("Official SDK head CAS conflict and uncertain hidden retry passed");
  }

  private static void reject(HTTPClient client, UpdateTableRequest request, String identity, int status) {
    ErrorHandler official = (ErrorHandler) ErrorHandlers.tableCommitHandler();
    String[] body = {null};
    try {
      client.post(PATH, request, LoadTableResponse.class, Map.of("Idempotency-Key", identity),
          new ErrorHandler() {
            @Override
            public ErrorResponse parseResponse(int code, String json) {
              require(code == status, "HTTP conflict status");
              body[0] = json;
              return official.parseResponse(code, json);
            }

            @Override
            public void accept(ErrorResponse error) {
              require(error.code() == status, "wire conflict status");
              require((status == 409 ? "CommitFailedException" : "ServiceUnavailableException")
                  .equals(error.type()), "wire failure type");
              official.accept(error);
            }
          });
    } catch (CommitFailedException | CommitStateUnknownException expected) {
      require(body[0] != null, "server-originated conflict");
      require(status == 409 ? expected instanceof CommitFailedException
          : expected instanceof CommitStateUnknownException, "official exception type");
      return;
    }
    throw new AssertionError("CAS loser must fail, not rebase or succeed");
  }

  private static void require(boolean condition, String message) {
    if (!condition) {
      throw new AssertionError(message);
    }
  }
}
