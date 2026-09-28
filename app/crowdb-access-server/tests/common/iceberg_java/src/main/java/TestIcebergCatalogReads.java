import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.rest.RESTCatalog;

public final class TestIcebergCatalogReads {
  public static void main(String[] args) throws Exception {
    Namespace namespace = Namespace.of("analytics");
    TableIdentifier events = TableIdentifier.of(namespace, "events");
    for (String mode : List.of("all", "refs")) {
      Map<String, String> properties = new HashMap<>();
      properties.put("uri", args[0]);
      properties.put("token", "r".repeat(32));
      properties.put("io-impl", TestNoFileIO.class.getName());
      properties.put("snapshot-loading-mode", mode);
      properties.put("rest-page-size", "1");
      properties.put("rest-metrics-reporting-enabled", "false");
      try (RESTCatalog catalog = new RESTCatalog()) {
        catalog.initialize("crowdb", properties);
        List<TableIdentifier> tables = catalog.listTables(namespace);
        require(tables.size() == 3 && tables.contains(events), "paged list");
        require(catalog.tableExists(events), "exists");
        require(!catalog.tableExists(TableIdentifier.of(namespace, "absent")), "missing table");
        Table table = catalog.loadTable(events);
        require(table.currentSnapshot().snapshotId() == 20, "current snapshot");
        require(table.refs().get("tag").snapshotId() == 30, "tag reference");
        Table unchanged = catalog.loadTable(events);
        require(unchanged.currentSnapshot().snapshotId() == 20, "conditional load");
        int count = 0;
        for (Snapshot snapshot : table.snapshots()) {
          require(snapshot.schemaId() == 0, "snapshot schema");
          count++;
        }
        require(count == 3, "complete snapshots including REFS fallback");
        for (String name : List.of("a+b", "%2F")) {
          require(catalog.loadTable(TableIdentifier.of(namespace, name)).schema().columns().size() == 1,
              "single-decoded table name");
        }
      }
    }
    System.out.println("Official RESTCatalog read acceptance passed");
  }

  private static void require(boolean valid, String operation) {
    if (!valid) {
      throw new IllegalStateException("RESTCatalog failed: " + operation);
    }
  }

  public static final class TestNoFileIO implements FileIO {
    public TestNoFileIO() {}

    @Override
    public InputFile newInputFile(String path) {
      throw new AssertionError("Catalog read unexpectedly accessed a file: " + path);
    }

    @Override
    public OutputFile newOutputFile(String path) {
      throw new AssertionError("Read-only acceptance attempted to create a file");
    }

    @Override
    public void deleteFile(String path) {
      throw new AssertionError("Read-only acceptance attempted to delete a file");
    }

    @Override
    public Map<String, String> properties() {
      return Map.of();
    }
  }
}
