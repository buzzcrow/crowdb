import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.types.Types;

public final class TestMetadataFixtures {
  public static void main(String[] args) {
    Schema schema = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));
    for (int version = 1; version <= 3; version++) {
      TableMetadata metadata = TableMetadata.newTableMetadata(
          schema, PartitionSpec.unpartitioned(), args[0], Map.of("format-version", String.valueOf(version)));
      String json = TableMetadataParser.toJson(metadata);
      TableMetadataParser.fromJson(json);
      System.out.println("METADATA_V" + version + "="
          + Base64.getEncoder().encodeToString(json.getBytes(StandardCharsets.UTF_8)));
    }
  }
}
