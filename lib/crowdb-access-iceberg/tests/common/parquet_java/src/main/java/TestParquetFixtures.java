import java.nio.file.Path;
import java.util.Base64;
import org.apache.iceberg.Files;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.parquet.Parquet;

public class TestParquetFixtures {
  public static void main(String[] args) throws Exception {
    String target = args[0];
    for (String version : new String[] {"v1", "v2"}) {
      Path file = java.nio.file.Files.createTempFile("iceberg-position-", ".parquet");
      try {
        try (var writer = Parquet.writeDeletes(Files.localOutput(file.toFile()))
            .overwrite()
            .withSpec(PartitionSpec.unpartitioned())
            .set("write.delete.parquet.compression-codec", "zstd")
            .set("write.delete.parquet.page-version", version)
            .buildPositionWriter()) {
          for (long position = 0; position < 100; position++) {
            writer.write(PositionDelete.create().set(target, position));
          }
        }
        System.out.println(version + "=" + Base64.getEncoder().encodeToString(java.nio.file.Files.readAllBytes(file)));
      } finally {
        java.nio.file.Files.deleteIfExists(file);
      }
    }
  }
}
