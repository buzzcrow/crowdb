import java.lang.reflect.Proxy;
import java.util.Base64;
import java.util.Map;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.PartitionStatsHandler;
import org.apache.iceberg.Partitioning;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.parquet.ParquetSchemaUtil;
import org.apache.iceberg.types.Types;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;

public final class TestPartitionStatisticsFixtures {
  public static void main(String[] args) throws Exception {
    var historical = new Schema(
        Types.NestedField.optional(1, "old", Types.LongType.get()),
        Types.NestedField.optional(2, "kept", Types.IntegerType.get()));
    var original = PartitionSpec.builderFor(historical)
        .identity("old", "old_part").identity("kept", "kept_part").build();
    for (boolean deleted : new boolean[] {false, true}) {
      var current = deleted
          ? new Schema(Types.NestedField.optional(2, "kept", Types.IntegerType.get()))
          : historical;
      var latest = PartitionSpecParser.fromJson(current,
          "{\"spec-id\":1,\"fields\":[{\"source-id\":2,\"field-id\":1001,"
              + "\"name\":\"renamed\",\"transform\":\"identity\"}]}");
      var table = (Table) Proxy.newProxyInstance(Table.class.getClassLoader(),
          new Class<?>[] {Table.class}, (proxy, method, arguments) -> {
            return switch (method.getName()) {
              case "schema" -> current;
              case "specs" -> Map.of(0, original, 1, latest);
              default -> throw new UnsupportedOperationException(method.getName());
            };
          });
      var partition = Partitioning.partitionType(table);
      for (int version : new int[] {2, 3}) {
        var schema = ParquetSchemaUtil.convert(PartitionStatsHandler.schema(partition, version), "stats");
        var file = java.nio.file.Files.createTempFile("partition-stats-", ".parquet");
        try {
          try (var writer = ExampleParquetWriter.builder(new Path(file.toUri()))
              .withType(schema).withWriteMode(ParquetFileWriter.Mode.OVERWRITE).build()) {
            var row = new SimpleGroupFactory(schema).newGroup();
            var tuple = row.addGroup("partition");
            if (!deleted) {
              tuple.append("old_part", 11L);
            }
            tuple.append("renamed", 7);
            for (var field : schema.getFields()) {
              if (field.isPrimitive()) {
                var name = field.getName();
                switch (field.asPrimitiveType().getPrimitiveTypeName()) {
                  case INT32 -> row.append(name, name.equals("spec_id") ? 1 : 0);
                  case INT64 -> row.append(name, 0L);
                  default -> throw new IllegalStateException(name);
                }
              }
            }
            writer.write(row);
          }
          System.out.println("FIXTURE STATS_" + version + "_" + deleted + "="
              + Base64.getEncoder().encodeToString(java.nio.file.Files.readAllBytes(file)));
        } finally {
          java.nio.file.Files.deleteIfExists(file);
        }
      }
    }
  }
}
