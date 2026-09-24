import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.aws.AwsClientProperties;
import org.apache.iceberg.aws.s3.VendedCredentialsProvider;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.types.Types;

public final class TestIcebergCatalogWrites {
  public static void main(String[] args) throws Exception {
    Schema schema = new Schema(Types.NestedField.required(91, "id", Types.LongType.get()));
    try (RESTCatalog catalog = new RESTCatalog()) {
      catalog.initialize("crowdb", Map.of("uri", args[0], "token", "w".repeat(32),
          "io-impl", "org.apache.iceberg.aws.s3.S3FileIO", "client.region", "us-east-1",
          "rest-metrics-reporting-enabled", "false"));
      if (args.length > 1 && args[1].equals("verify")) {
        for (String tableName : new String[] {"immediate", "staged"}) {
          Table persisted = catalog.loadTable(TableIdentifier.of(Namespace.of("analytics"), tableName));
          credential(persisted);
          verifyFiles(persisted, tableName.equals("immediate") ? 2 : 1);
        }
        System.out.println("Official RESTCatalog restart read and credential acceptance passed");
        return;
      }
      TableIdentifier name = TableIdentifier.of(Namespace.of("analytics"), "immediate");
      Table table = catalog.buildTable(name, schema).withProperty("format-version", "1").create();
      require(table.schema().findField("id").fieldId() == 1, "fresh create field IDs");
      if (args.length > 1) {
        table.newAppend().appendFile(writeData(table)).commit();
        verifyFiles(table, 1);
      }
      table.updateProperties().set("owner", "sdk").commit();
      table.updateSchema().addColumn("message", Types.StringType.get()).commit();
      table.updateProperties().set("format-version", "3").commit();
      Table loaded = catalog.loadTable(name);
      require(loaded.schema().findField("message") != null, "ordered schema commit");
      require(loaded.properties().get("owner").equals("sdk"), "property commit");
      credential(loaded);
      if (args.length > 1) {
        loaded.newAppend().appendFile(writeData(loaded)).commit();
        verifyFiles(loaded, 2);
      }
      TableIdentifier stagedName = TableIdentifier.of(Namespace.of("analytics"), "staged");
      Transaction first = catalog.buildTable(stagedName, schema).createTransaction();
      Transaction second = catalog.buildTable(stagedName, schema).createTransaction();
      require(!catalog.tableExists(stagedName), "draft invisibility");
      require(!first.table().location().equals(second.table().location()), "same-name draft isolation");
      String firstCredential = credential(first.table());
      String secondCredential = credential(second.table());
      require(!firstCredential.equals(secondCredential), "independent vended credentials");
      first.updateProperties().set("draft-owner", "sdk").commit();
      if (args.length > 1) {
        first.newAppend().appendFile(writeData(first.table())).commit();
      }
      first.commitTransaction();
      require(catalog.loadTable(stagedName).properties().get("draft-owner").equals("sdk"), "staged publication");
      credential(first.table());
      if (args.length > 1) {
        verifyFiles(catalog.loadTable(stagedName), 1);
      }
      System.out.println("Official RESTCatalog create, update, upgrade, stage and refresh acceptance passed");
    }
  }

  private static DataFile writeData(Table table) throws Exception {
    String path = table.location() + "/data/" + UUID.randomUUID() + ".parquet";
    DataWriter<Record> writer = Parquet.writeData(table.io().newOutputFile(path))
        .schema(table.schema()).withSpec(table.spec())
        .createWriterFunc(parquetSchema -> GenericParquetWriter.create(table.schema(), parquetSchema))
        .set("write.parquet.compression-codec", "zstd").build();
    try (writer) {
      for (long row = 0; row < 10; row++) {
        GenericRecord record = GenericRecord.create(table.schema());
        record.setField("id", row);
        if (table.schema().findField("message") != null) {
          record.setField("message", "row-" + row);
        }
        writer.write(record);
      }
    }
    return writer.toDataFile();
  }

  private static void verifyFiles(Table table, int expectedFiles) throws Exception {
    int count = 0;
    try (var tasks = table.newScan().planFiles()) {
      for (FileScanTask task : tasks) {
        require(task.file().recordCount() == 10, "selected manifest rows");
        try (var input = table.io().newInputFile(task.file().location()).newStream()) {
          require(new String(input.readNBytes(4), java.nio.charset.StandardCharsets.US_ASCII).equals("PAR1"),
              "native Parquet read");
        }
        count++;
      }
    }
    require(count == expectedFiles, "selected data file count");
  }

  private static String credential(Table table) {
    try (VendedCredentialsProvider provider = (VendedCredentialsProvider)
        new AwsClientProperties(table.io().properties()).credentialsProvider(null, null, null)) {
      return provider.resolveCredentials().accessKeyId();
    }
  }

  private static void require(boolean valid, String message) {
    if (!valid) {
      throw new IllegalStateException(message);
    }
  }
}
