import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.ImmutableGenericPartitionStatisticsFile;
import org.apache.iceberg.SnapshotParser;
import org.apache.iceberg.Schema;
import org.apache.iceberg.UpdateRequirement;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.rest.ErrorHandlers;
import org.apache.iceberg.rest.ErrorHandler;
import org.apache.iceberg.rest.HTTPClient;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.rest.auth.AuthSession;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.apache.iceberg.types.Types;

public final class TestIcebergCommitErrors {
  private static final TableIdentifier NAME = TableIdentifier.of("analytics", "commit_errors");
  private static final String PATH = "v1/namespaces/analytics/tables/commit_errors";

  public static void main(String[] args) throws Exception {
    Map<String, String> properties = Map.of("uri", args[0], "token", "w".repeat(32),
        "io-impl", TestIcebergCatalogReads.TestNoFileIO.class.getName(),
        "rest-metrics-reporting-enabled", "false");
    Schema schema = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));
    try (RESTCatalog catalog = new RESTCatalog();
        HTTPClient root = HTTPClient.builder(properties).uri(args[0])
            .withHeaders(Map.of("Authorization", "Bearer " + "w".repeat(32))).build();
        HTTPClient client = root.withAuthSession(AuthSession.EMPTY)) {
      catalog.initialize("crowdb", properties);
      catalog.buildTable(NAME, schema).withProperty("format-version", "2").create();
      String initial = metadata(catalog);
      expect(AlreadyExistsException.class, () -> catalog.buildTable(NAME, schema).create());
      require(initial.equals(metadata(catalog)), "duplicate create preserves head");
      String uuid = ((BaseTable) catalog.loadTable(NAME)).operations().current().uuid();
      rejected(catalog, client, new UpdateTableRequest(
          List.of(new UpdateRequirement.AssertTableUUID("00000000-0000-0000-0000-000000000000")),
          List.of(property("invalid"))), 409, "CommitFailedException", CommitFailedException.class);
      rejected(catalog, client, new UpdateTableRequest(List.of(),
          List.of(property("partial"), new MetadataUpdate.SetCurrentSchema(999))),
          400, "BadRequestException", BadRequestException.class);
      rejected(catalog, client, new UpdateTableRequest(List.of(),
          List.of(new MetadataUpdate.UpgradeFormatVersion(99))),
          400, "BadRequestException", BadRequestException.class);
      rejected(catalog, client, UpdateTableRequest.create(TableIdentifier.of("analytics", "other"),
          List.of(), List.of(property("wrong-path"))),
          400, "BadRequestException", BadRequestException.class);
      unavailablePartitionStatistics(catalog, client);
      counts(catalog, client, uuid);
      int oldSchema = catalog.loadTable(NAME).schema().schemaId();
      catalog.loadTable(NAME).updateSchema().addColumn("message", Types.StringType.get()).commit();
      rejected(catalog, client, new UpdateTableRequest(
          List.of(new UpdateRequirement.AssertCurrentSchemaID(oldSchema)), List.of(property("stale"))),
          409, "CommitFailedException", CommitFailedException.class);
      require(catalog.dropTable(NAME, false), "drop succeeds");
      failure(client, new UpdateTableRequest(List.of(), List.of(property("dropped"))),
          404, "NoSuchTableException", NoSuchTableException.class);
      catalog.buildTable(NAME, schema).create();
      rejected(catalog, client, new UpdateTableRequest(
          List.of(new UpdateRequirement.AssertTableUUID(uuid)), List.of(property("old-identity"))),
          409, "CommitFailedException", CommitFailedException.class);
    }
    System.out.println("Official commit errors, atomic rejection and count boundaries passed");
  }

  private static void unavailablePartitionStatistics(RESTCatalog catalog, HTTPClient client) {
    String location = catalog.loadTable(NAME).location();
    var snapshot = SnapshotParser.fromJson("{\"snapshot-id\":1,\"sequence-number\":1,"
        + "\"timestamp-ms\":" + System.currentTimeMillis()
        + ",\"schema-id\":0,\"summary\":{\"operation\":\"append\"},"
        + "\"manifest-list\":\"" + location + "/metadata/disabled.avro\"}");
    var statistics = ImmutableGenericPartitionStatisticsFile.builder().snapshotId(1)
        .path(location + "/metadata/disabled.parquet").fileSizeInBytes(8).build();
    rejected(catalog, client, new UpdateTableRequest(List.of(), List.of(property("disabled"),
        new MetadataUpdate.AddSnapshot(snapshot), new MetadataUpdate.SetPartitionStatistics(statistics))),
        400, "BadRequestException", BadRequestException.class);
  }

  private static void counts(RESTCatalog catalog, HTTPClient client, String uuid) {
    List<UpdateRequirement> requirements = Collections.nCopies(1000,
        new UpdateRequirement.AssertCurrentSchemaID(catalog.loadTable(NAME).schema().schemaId()));
    List<MetadataUpdate> updates = Collections.nCopies(1000, property("at-limit"));
    client.post(PATH, new UpdateTableRequest(requirements, updates), LoadTableResponse.class,
        Map.of(), ErrorHandlers.tableCommitHandler());
    require("at-limit".equals(catalog.loadTable(NAME).properties().get("boundary")),
        "exact requirement and update count limits accepted");
    rejected(catalog, client, new UpdateTableRequest(Collections.nCopies(1001,
        new UpdateRequirement.AssertCurrentSchemaID(catalog.loadTable(NAME).schema().schemaId())),
        List.of(property("over-requirements"))),
        400, "BadRequestException", BadRequestException.class);
    rejected(catalog, client, new UpdateTableRequest(Collections.nCopies(1000,
        new UpdateRequirement.AssertTableUUID(uuid)), List.of(property("over-text"))),
        400, "BadRequestException", BadRequestException.class);
    client.post(PATH, new UpdateTableRequest(
        List.of(new UpdateRequirement.AssertRefSnapshotID("a".repeat(4096), null)),
        List.of(property("at-text-limit"))), LoadTableResponse.class,
        Map.of(), ErrorHandlers.tableCommitHandler());
    require("at-text-limit".equals(catalog.loadTable(NAME).properties().get("boundary")),
        "exact requirement text budget accepted");
    rejected(catalog, client, new UpdateTableRequest(
        List.of(new UpdateRequirement.AssertRefSnapshotID("a".repeat(4097), null)),
        List.of(property("over-text-limit"))), 400, "BadRequestException", BadRequestException.class);
    rejected(catalog, client, new UpdateTableRequest(List.of(),
        Collections.nCopies(1001, property("over-updates"))),
        400, "BadRequestException", BadRequestException.class);
  }

  private static MetadataUpdate property(String value) {
    return new MetadataUpdate.SetProperties(Map.of("boundary", value));
  }

  private static String metadata(RESTCatalog catalog) {
    return ((BaseTable) catalog.loadTable(NAME)).operations().current().metadataFileLocation();
  }

  private static void rejected(RESTCatalog catalog, HTTPClient client, UpdateTableRequest request,
      int status, String type, Class<? extends RuntimeException> exception) {
    String before = metadata(catalog);
    failure(client, request, status, type, exception);
    require(before.equals(metadata(catalog)), "rejected commit preserves selected metadata");
  }

  private static void failure(HTTPClient client, UpdateTableRequest request, int status,
      String type, Class<? extends RuntimeException> exception) {
    boolean[] received = {false};
    ErrorHandler official = (ErrorHandler) ErrorHandlers.tableCommitHandler();
    expect(exception, () -> client.post(PATH, request, LoadTableResponse.class, Map.of(), new ErrorHandler() {
      @Override
      public ErrorResponse parseResponse(int code, String json) {
        require(code == status, "expected HTTP status " + status + ", received " + code);
        return official.parseResponse(code, json);
      }

      @Override
      public void accept(ErrorResponse error) {
        received[0] = true;
        require(error.code() == status, "expected status " + status + ", received " + error);
        require(type.equals(error.type()), "expected error type " + type + ", received " + error);
        official.accept(error);
      }
    }));
    require(received[0], "exception must originate from server response");
  }

  private static void expect(Class<? extends RuntimeException> expected, Runnable action) {
    try {
      action.run();
    } catch (RuntimeException failure) {
      if (failure.getClass().equals(expected)) {
        return;
      }
      throw failure;
    }
    throw new AssertionError("Expected " + expected.getSimpleName());
  }

  private static void require(boolean condition, String message) {
    if (!condition) {
      throw new AssertionError(message);
    }
  }
}
