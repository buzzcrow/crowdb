import java.util.Map;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.types.Types;

public final class TestIcebergResponseLoss {
  public static void main(String[] args) throws Exception {
    TableIdentifier table = TableIdentifier.of(Namespace.of("analytics"), "java_lost_reply");
    Schema schema = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));
    try (RESTCatalog first = new RESTCatalog(); RESTCatalog second = new RESTCatalog()) {
      first.initialize("crowdb", Map.of("uri", args[0], "token", "w".repeat(32),
          "io-impl", "org.apache.iceberg.aws.s3.S3FileIO", "rest-metrics-reporting-enabled", "false"));
      second.initialize("crowdb", Map.of("uri", args[1], "token", "w".repeat(32),
          "io-impl", "org.apache.iceberg.aws.s3.S3FileIO", "rest-metrics-reporting-enabled", "false"));
      boolean failed = false;
      try {
        first.buildTable(table, schema).create();
      } catch (RuntimeException expected) {
        failed = true;
      }
      if (!failed || !second.tableExists(table)) {
        throw new AssertionError("lost Java create response did not preserve one visible table");
      }
      second.loadTable(table);
      if (!second.dropTable(table)) {
        throw new AssertionError("second listener could not drop the committed table");
      }
      System.out.println("Official Java RESTCatalog response-loss acceptance passed");
    }
  }
}
