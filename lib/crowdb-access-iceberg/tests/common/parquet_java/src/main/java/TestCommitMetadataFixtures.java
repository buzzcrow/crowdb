import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.MetadataUpdateParser;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.types.Types;

public final class TestCommitMetadataFixtures {
  public static void main(String[] args) {
    Schema schema = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));
    List<String> updates = List.of(
        "{\"action\":\"add-schema\",\"last-column-id\":999,\"schema\":{\"type\":\"struct\",\"schema-id\":999,\"fields\":["
            + "{\"id\":1,\"name\":\"renamed\",\"required\":true,\"type\":\"long\"},"
            + "{\"id\":2,\"name\":\"added\",\"required\":false,\"type\":\"string\"}]}}",
        "{\"action\":\"set-current-schema\",\"schema-id\":-1}",
        "{\"action\":\"add-spec\",\"spec\":{\"spec-id\":999,\"fields\":["
            + "{\"source-id\":1,\"field-id\":1000,\"name\":\"bucket\",\"transform\":\"bucket[16]\"}]}}",
        "{\"action\":\"set-default-spec\",\"spec-id\":-1}",
        "{\"action\":\"add-sort-order\",\"sort-order\":{\"order-id\":999,\"fields\":["
            + "{\"source-id\":2,\"transform\":\"identity\",\"direction\":\"asc\",\"null-order\":\"nulls-last\"}]}}",
        "{\"action\":\"set-default-sort-order\",\"sort-order-id\":-1}",
        "{\"action\":\"set-properties\",\"updates\":{\"owner\":\"one\",\"remove\":\"yes\"}}",
        "{\"action\":\"remove-properties\",\"removals\":[\"remove\"]}",
        "{\"action\":\"set-properties\",\"updates\":{\"owner\":\"two\"}}");
    for (int version = 1; version <= 3; version++) {
      TableMetadata initial = TableMetadata.newTableMetadata(schema, PartitionSpec.unpartitioned(),
          args[0], Map.of("format-version", String.valueOf(version)));
      String input = TableMetadataParser.toJson(initial);
      TableMetadata base = TableMetadataParser.fromJson(input);
      TableMetadata.Builder builder = TableMetadata.buildFrom(base)
          .setPreviousFileLocation(args[0] + "metadata/one.metadata.json");
      for (String update : updates) {
        MetadataUpdateParser.fromJson(update).applyTo(builder);
      }
      TableMetadata result = builder.build();
      String output = TableMetadataParser.toJson(result);
      TableMetadataParser.fromJson(output);
      emit("INPUT_V" + version, input);
      emit("REQUEST_V" + version, "{\"requirements\":[],\"updates\":[" + String.join(",", updates) + "]}");
      emit("OUTPUT_V" + version, output);
    }
  }

  private static void emit(String name, String value) {
    System.out.println(name + "=" + Base64.getEncoder().encodeToString(value.getBytes(StandardCharsets.UTF_8)));
  }
}
