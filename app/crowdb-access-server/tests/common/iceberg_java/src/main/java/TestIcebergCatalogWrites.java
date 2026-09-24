import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.BaseTable;
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
        TestIcebergPartitionStatistics.run(catalog, args[0], true);
        for (String tableName : new String[] {"immediate", "staged"}) {
          Table persisted = catalog.loadTable(TableIdentifier.of(Namespace.of("analytics"), tableName));
          credential(persisted);
          verifyFiles(persisted, tableName.equals("immediate") ? 2 : 1);
        }
        require(!catalog.tableExists(TableIdentifier.of(Namespace.of("analytics"), "lifecycle")),
            "dropped lifecycle table remains absent after restart");
        require(!catalog.namespaceExists(Namespace.of("lifecycle_destination")),
            "empty destination namespace remains dropped after restart");
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
      lifecycle(catalog, schema, args.length > 1);
      if (args.length > 1) {
        TestIcebergPartitionStatistics.run(catalog, args[0], false);
      }
      System.out.println("Official RESTCatalog create, update, upgrade, stage, refresh, rename and drop acceptance passed");
    }
  }

  private static void lifecycle(RESTCatalog catalog, Schema schema, boolean nativeFiles) throws Exception {
    Namespace destination = Namespace.of("lifecycle_destination");
    catalog.createNamespace(destination);
    TableIdentifier source = TableIdentifier.of(Namespace.of("analytics"), "lifecycle");
    TableIdentifier renamed = TableIdentifier.of(Namespace.of("analytics"), "lifecycle_renamed");
    TableIdentifier moved = TableIdentifier.of(destination, "moved");
    Table original = catalog.buildTable(source, schema).create();
    if (nativeFiles) {
      original.newAppend().appendFile(writeData(original)).commit();
    }
    String location = original.location();
    catalog.renameTable(source, renamed);
    require(!catalog.tableExists(source), "old name is not an alias");
    require(catalog.loadTable(renamed).location().equals(location), "rename preserves file location");
    catalog.renameTable(renamed, moved);
    require(!catalog.tableExists(renamed), "cross-namespace old name is not an alias");
    Table selected = catalog.loadTable(moved);
    require(selected.location().equals(location), "cross-namespace stable identity");
    credential(selected);
    selected.updateProperties().set("after-rename", "yes").commit();
    require(catalog.loadTable(moved).properties().get("after-rename").equals("yes"), "commit after rename");
    if (nativeFiles) {
      verifyFiles(selected, 1);
    }
    String metadata = ((BaseTable) selected).operations().current().metadataFileLocation();
    require(catalog.dropTable(moved, false), "logical drop succeeds");
    require(!catalog.tableExists(moved), "dropped table is absent");
    require(catalog.dropNamespace(destination), "moved table does not leave live namespace children");
    if (nativeFiles) {
      require(selected.io().newInputFile(metadata).exists(), "drop does not physically delete metadata");
      verifyFiles(selected, 1);
    }
    Table recreated = catalog.buildTable(source, schema).create();
    require(!recreated.location().equals(location), "recreated name uses a new table identity");
    String recreatedMetadata = ((BaseTable) recreated).operations().current().metadataFileLocation();
    if (nativeFiles) {
      require(recreated.io().newInputFile(recreatedMetadata).exists(), "metadata exists before purge request");
    }
    require(catalog.dropTable(source, true), "purge request logically drops the table");
    require(!catalog.tableExists(source), "purge request removes name visibility");
    if (nativeFiles) {
      require(recreated.io().newInputFile(recreatedMetadata).exists(), "purge is a deferred proof task");
    }
    require(!catalog.dropTable(source, false), "missing table follows SDK false contract");
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
