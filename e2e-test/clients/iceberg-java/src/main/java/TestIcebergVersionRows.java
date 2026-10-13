import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.types.Types;

public final class TestIcebergVersionRows {
  public static void run(RESTCatalog catalog, boolean verifyOnly) throws Exception {
    Schema schema = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));
    for (int version = 1; version <= 3; version++) {
      TableIdentifier name = TableIdentifier.of("analytics", "rows_v" + version);
      Table table;
      if (verifyOnly) {
        table = catalog.loadTable(name);
      } else {
        table = catalog.buildTable(name, schema)
            .withProperty("format-version", Integer.toString(version)).create();
        table.newAppend().appendFile(write(table, 10L)).commit();
        long first = table.currentSnapshot().snapshotId();
        table.newAppend().appendFile(write(table, 20L)).commit();
        rows(IcebergGenerics.read(table).useSnapshot(first), List.of(10L));
        rows(IcebergGenerics.read(table), List.of(10L, 20L));
        for (int upgrade = version + 1; upgrade <= 3; upgrade++) {
          table.updateProperties().set("format-version", Integer.toString(upgrade)).commit();
          table = catalog.loadTable(name);
          rows(IcebergGenerics.read(table), List.of(10L, 20L));
          rows(IcebergGenerics.read(table).useSnapshot(first), List.of(10L));
        }
        table.expireSnapshots().expireSnapshotId(first).cleanExpiredFiles(false).commit();
        table.refresh();
        require(table.snapshot(first) == null, "logical expiry removes the old snapshot");
        table.updateProperties().set("expired-snapshot", Long.toString(first)).commit();
      }
      long expired = Long.parseLong(table.properties().get("expired-snapshot"));
      require(table.snapshot(expired) == null, "expired snapshot remains absent after reload");
      rows(IcebergGenerics.read(table), List.of(10L, 20L));
    }
  }

  private static DataFile write(Table table, long value) throws Exception {
    DataWriter<Record> writer = Parquet.writeData(table.io().newOutputFile(
        table.location() + "/data/" + UUID.randomUUID() + ".parquet"))
        .schema(table.schema()).withSpec(table.spec())
        .createWriterFunc(parquet -> GenericParquetWriter.create(table.schema(), parquet)).build();
    try (writer) {
      GenericRecord row = GenericRecord.create(table.schema());
      row.setField("id", value);
      writer.write(row);
    }
    return writer.toDataFile();
  }

  static void rows(IcebergGenerics.ScanBuilder scan, List<Long> expected) throws Exception {
    List<Long> actual = new ArrayList<>();
    try (var rows = scan.build()) {
      for (Record row : rows) {
        actual.add((Long) row.getField("id"));
      }
    }
    actual.sort(Long::compareTo);
    require(actual.equals(expected), "visible rows: expected " + expected + ", got " + actual);
  }

  private static void require(boolean condition, String message) {
    if (!condition) {
      throw new AssertionError(message);
    }
  }
}
