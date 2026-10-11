import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionStatisticsFile;
import org.apache.iceberg.PartitionStatsHandler;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.UpdateRequirement;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.rest.ErrorHandlers;
import org.apache.iceberg.rest.HTTPClient;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.rest.auth.AuthSession;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.types.Types;

public final class TestIcebergPartitionStatistics {
  private static final TableIdentifier NAME = TableIdentifier.of("analytics", "partition_statistics");

  public static void run(RESTCatalog catalog, String endpoint, boolean verifyOnly) throws Exception {
    if (verifyOnly) {
      verify(catalog.loadTable(NAME), 20, 2);
      verify(catalog.loadTable(TableIdentifier.of("analytics", "staged_statistics")), 10, 1);
      return;
    }
    Schema schema = new Schema(
        Types.NestedField.required(1, "id", Types.LongType.get()),
        Types.NestedField.optional(2, "category", Types.StringType.get()));
    PartitionSpec spec = PartitionSpec.builderFor(schema).identity("id").build();
    Table table = catalog.buildTable(NAME, schema).withPartitionSpec(spec)
        .withProperty("format-version", "2").create();
    table.newAppend().appendFile(writeData(table)).commit();
    PartitionStatisticsFile statistics = PartitionStatsHandler.computeAndWriteStatsFile(table);
    publishAndReplay(table, endpoint, statistics);
    verify(table, 10, 1);
    table.updateSchema().addColumn("extra", Types.StringType.get()).commit();
    table.updateSpec().addField(Expressions.bucket("category", 8)).commit();
    table.updateProperties().set("format-version", "3").commit();
    verify(table, 10, 1);
    table.newAppend().appendFile(writeData(table)).commit();
    table.updatePartitionStatistics()
        .setPartitionStatistics(PartitionStatsHandler.computeAndWriteStatsFile(table)).commit();
    verify(table, 20, 2);
    var transaction = catalog.buildTable(TableIdentifier.of("analytics", "staged_statistics"), schema)
        .withPartitionSpec(spec).withProperty("format-version", "3").createTransaction();
    transaction.newAppend().appendFile(writeData(transaction.table())).commit();
    transaction.updatePartitionStatistics().setPartitionStatistics(
        PartitionStatsHandler.computeAndWriteStatsFile(transaction.table())).commit();
    transaction.commitTransaction();
    verify(catalog.loadTable(TableIdentifier.of("analytics", "staged_statistics")), 10, 1);
    System.out.println("Official partition statistics publication, replay, evolution and staged creation passed");
  }

  private static void publishAndReplay(Table table, String endpoint, PartitionStatisticsFile statistics)
      throws java.io.IOException {
    String path = "v1/namespaces/analytics/tables/partition_statistics";
    var request = new UpdateTableRequest(
        List.of(new UpdateRequirement.AssertTableUUID(((BaseTable) table).operations().current().uuid())),
        List.of(new MetadataUpdate.SetPartitionStatistics(statistics)));
    UUID random = UUID.randomUUID();
    UUID identity = new UUID((System.currentTimeMillis() << 16) | 0x7000
        | (random.getMostSignificantBits() & 0xfff), random.getLeastSignificantBits());
    Map<String, String> headers = Map.of("Idempotency-Key", identity.toString());
    try (HTTPClient root = HTTPClient.builder(Map.of()).uri(endpoint)
        .withHeaders(Map.of("Authorization", "Bearer " + "w".repeat(32))).build();
        HTTPClient client = root.withAuthSession(AuthSession.EMPTY)) {
      LoadTableResponse first = client.post(path, request, LoadTableResponse.class, headers,
          ErrorHandlers.tableCommitHandler());
      LoadTableResponse replay = client.post(path, request, LoadTableResponse.class, headers,
          ErrorHandlers.tableCommitHandler());
      require(first.metadataLocation().equals(replay.metadataLocation()), "exact statistics publication replay");
    }
    table.refresh();
  }

  private static DataFile writeData(Table table) throws Exception {
    GenericRecord row = GenericRecord.create(table.schema());
    row.setField("id", 7L);
    row.setField("category", "same");
    PartitionKey partition = new PartitionKey(table.spec(), table.schema());
    partition.partition(row);
    DataWriter<Record> writer = Parquet.writeData(table.io().newOutputFile(
        table.location() + "/data/" + UUID.randomUUID() + ".parquet"))
        .schema(table.schema()).withSpec(table.spec()).withPartition(partition)
        .createWriterFunc(parquetSchema -> GenericParquetWriter.create(table.schema(), parquetSchema))
        .set("write.parquet.compression-codec", "zstd").build();
    try (writer) {
      for (int index = 0; index < 10; index++) {
        writer.write(row);
      }
    }
    return writer.toDataFile();
  }

  private static void verify(Table table, long expectedRecords, int expectedFiles) throws Exception {
    long records = 0;
    int files = 0;
    try (var statistics = table.newPartitionStatisticsScan().scan()) {
      for (var row : statistics) {
        records += row.dataRecordCount();
        files += row.dataFileCount();
        require(row.dvCount() == null || row.dvCount() == 0, "missing historical DV defaults to zero");
      }
    }
    require(records == expectedRecords && files == expectedFiles, "selected partition statistics counts");
  }

  private static void require(boolean valid, String message) {
    if (!valid) {
      throw new AssertionError(message);
    }
  }
}
