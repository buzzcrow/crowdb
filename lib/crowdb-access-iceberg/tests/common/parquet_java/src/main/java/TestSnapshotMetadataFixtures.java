import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotParser;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.types.Types;

public final class TestSnapshotMetadataFixtures {
  public static void main(String[] args) {
    Schema schema = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));
    for (int version = 1; version <= 3; version++) {
      TableMetadata metadata = TableMetadata.newTableMetadata(
          schema, PartitionSpec.unpartitioned(), args[0], Map.of("format-version", String.valueOf(version)));
      Snapshot first = snapshot(args[0], version, 10, 0, 1, 0, 2);
      metadata = TableMetadata.buildFrom(metadata).addSnapshot(first)
          .setRef("main", SnapshotRef.branchBuilder(10).build()).build();
      Snapshot second = snapshot(args[0], version, 20, 10, 2, 2, 3);
      metadata = TableMetadata.buildFrom(metadata).addSnapshot(second)
          .setRef("main", SnapshotRef.branchBuilder(20).build())
          .setRef("release", SnapshotRef.tagBuilder(10).maxRefAgeMs(86400000L).build()).build();
      String json = TableMetadataParser.toJson(metadata);
      TableMetadata parsed = TableMetadataParser.fromJson(json);
      if (parsed.currentSnapshot().snapshotId() != 20 || parsed.snapshots().size() != 2) {
        throw new IllegalStateException("Snapshot metadata round trip changed selection");
      }
      System.out.println("METADATA_SNAPSHOTS_V" + version + "="
          + Base64.getEncoder().encodeToString(json.getBytes(StandardCharsets.UTF_8)));
    }
  }

  private static Snapshot snapshot(String location, int version, long id, long parent,
      long sequence, long firstRow, long addedRows) {
    String json = "{\"snapshot-id\":" + id
        + (parent == 0 ? "" : ",\"parent-snapshot-id\":" + parent)
        + (version == 1 ? "" : ",\"sequence-number\":" + sequence)
        + ",\"timestamp-ms\":" + System.currentTimeMillis()
        + ",\"schema-id\":0,\"summary\":{\"operation\":\"append\"}"
        + ",\"manifest-list\":\"" + location + "metadata/snapshot-" + id + ".avro\""
        + (version == 3 ? ",\"first-row-id\":" + firstRow + ",\"added-rows\":" + addedRows : "")
        + "}";
    return SnapshotParser.fromJson(json);
  }
}
