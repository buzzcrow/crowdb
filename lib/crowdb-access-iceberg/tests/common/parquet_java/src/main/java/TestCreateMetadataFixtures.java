package org.apache.iceberg.rest;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.NullOrder;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.types.Types;

public final class TestCreateMetadataFixtures {
  public static void main(String[] args) throws Exception {
    Schema schema = new Schema(71, List.of(
        Types.NestedField.required(90, "id", Types.LongType.get()),
        Types.NestedField.optional(80, "nested", Types.StructType.of(
            Types.NestedField.optional(60, "text", Types.StringType.get()),
            Types.NestedField.optional(50, "values", Types.ListType.ofOptional(45,
                Types.StructType.of(Types.NestedField.optional(40, "item", Types.LongType.get())))))),
        Types.NestedField.optional(30, "mapping", Types.MapType.ofOptional(20, 10,
            Types.StringType.get(), Types.StructType.of(
                Types.NestedField.optional(5, "entry", Types.LongType.get()))))), Set.of(90));
    PartitionSpec spec = PartitionSpec.builderFor(schema).withSpecId(17).bucket("id", 16).build();
    SortOrder order = SortOrder.builderFor(schema).withOrderId(81)
        .desc("nested.text", NullOrder.NULLS_LAST).build();
    for (int version = 1; version <= 3; version++) {
      Map<String, String> properties = Map.of("format-version", String.valueOf(version),
          "uuid", "ignored", "owner", "sdk",
          "write.metadata.metrics.column.nested.values.item", "counts");
      CreateTableRequest request = CreateTableRequest.builder().withName("events")
          .withSchema(schema).withPartitionSpec(spec).withWriteOrder(order)
          .setProperties(new HashMap<>(properties)).build();
      TableMetadata metadata = TableMetadata.newTableMetadata(schema, spec, order, args[0], properties);
      String output = TableMetadataParser.toJson(metadata);
      TableMetadataParser.fromJson(output);
      emit("REQUEST_V" + version, RESTObjectMapper.mapper().writeValueAsString(request));
      emit("OUTPUT_V" + version, output);
    }
  }

  private static void emit(String name, String value) {
    System.out.println(name + "=" + Base64.getEncoder().encodeToString(value.getBytes(StandardCharsets.UTF_8)));
  }
}
