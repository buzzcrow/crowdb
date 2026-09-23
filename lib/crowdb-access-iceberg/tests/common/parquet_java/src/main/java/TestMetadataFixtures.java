import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.SortOrder;
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
    Schema original = new Schema(
        Types.NestedField.required(1, "id", Types.LongType.get()),
        Types.NestedField.optional(2, "old", Types.StringType.get()));
    for (int version = 2; version <= 3; version++) {
      TableMetadata metadata = TableMetadata.newTableMetadata(
          original, PartitionSpec.builderFor(original).identity("old").build(),
          SortOrder.builderFor(original).asc("old").build(), args[0],
          Map.of("format-version", String.valueOf(version)));
      metadata = metadata.updatePartitionSpec(PartitionSpec.unpartitioned())
          .replaceSortOrder(SortOrder.unsorted()).updateSchema(schema);
      if (version == 3) {
        Schema defaults = SchemaParser.fromJson("{\"type\":\"struct\",\"schema-id\":2,\"fields\":["
            + "{\"id\":1,\"name\":\"id\",\"type\":\"long\",\"required\":true},"
            + "{\"id\":3,\"name\":\"amount\",\"type\":\"decimal(9,2)\",\"required\":false,\"initial-default\":\"12.34\",\"write-default\":\"23.45\"},"
            + "{\"id\":4,\"name\":\"clock\",\"type\":\"timestamp_ns\",\"required\":false,\"initial-default\":\"2024-01-02T03:04:05.123456789\"}]}" );
        metadata = metadata.updateSchema(defaults);
      }
      String json = TableMetadataParser.toJson(metadata);
      TableMetadataParser.fromJson(json);
      System.out.println("METADATA_EVOLVED_V" + version + "="
          + Base64.getEncoder().encodeToString(json.getBytes(StandardCharsets.UTF_8)));
    }
  }
}
