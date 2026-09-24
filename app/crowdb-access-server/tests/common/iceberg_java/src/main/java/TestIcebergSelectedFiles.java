import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.types.Types;

public final class TestIcebergSelectedFiles {
  public static void main(String[] args) throws Exception {
    try (RESTCatalog catalog = new RESTCatalog()) {
      catalog.initialize("crowdb", Map.of("uri", args[0], "token", "w".repeat(32),
          "io-impl", "org.apache.iceberg.aws.s3.S3FileIO", "client.region", "us-east-1",
          "rest-metrics-reporting-enabled", "false"));
      Schema schema = new Schema(
          Types.NestedField.required(1, "id", Types.LongType.get()),
          Types.NestedField.required(2, "message", Types.StringType.get()));
      Table table = catalog.buildTable(TableIdentifier.of("analytics", "selected_files"), schema)
          .withProperty("format-version", "2").create();
      DataFile data = data(table);
      DeleteFile equality = equality(table);
      require(table.io().newInputFile(data.location()).exists(), "ordinary data upload");
      require(table.io().newInputFile(equality.location()).exists(), "ordinary equality-delete upload");
      table.newAppend().appendFile(data).commit();
      DataFile wrongData = DataFiles.builder(table.spec()).withPath(equality.location())
          .withFormat("PARQUET").withFileSizeInBytes(equality.fileSizeInBytes())
          .withRecordCount(equality.recordCount()).build();
      rejected(table, () -> table.newAppend().appendFile(wrongData).commit());
      DeleteFile wrongPosition = FileMetadata.deleteFileBuilder(table.spec()).ofPositionDeletes()
          .withPath(equality.location()).withFormat("PARQUET")
          .withFileSizeInBytes(equality.fileSizeInBytes()).withRecordCount(equality.recordCount()).build();
      rejected(table, () -> table.newRowDelta().addDeletes(wrongPosition).commit());
      DeleteFile wrongEquality = FileMetadata.deleteFileBuilder(table.spec())
          .ofEqualityDeletes(table.schema().findField("message").fieldId())
          .withPath(equality.location()).withFormat("PARQUET")
          .withFileSizeInBytes(equality.fileSizeInBytes()).withRecordCount(equality.recordCount()).build();
      rejected(table, () -> table.newRowDelta().addDeletes(wrongEquality).commit());
      table.newRowDelta().addDeletes(equality).commit();
      table.refresh();
      int files = 0;
      try (var tasks = table.newScan().planFiles()) {
        for (var task : tasks) {
          require(task.file().location().equals(data.location()), "original data remains selected");
          require(task.deletes().size() == 1
              && task.deletes().get(0).location().equals(equality.location()), "equality delete is selected");
          files++;
        }
      }
      require(files == 1, "wrong uses never add files");
      System.out.println("Official identical S3 uploads and selected data/delete validation passed");
    }
  }

  private static DataFile data(Table table) throws Exception {
    DataWriter<Record> writer = Parquet.writeData(table.io().newOutputFile(location(table)))
        .schema(table.schema()).withSpec(table.spec())
        .createWriterFunc(parquet -> GenericParquetWriter.create(table.schema(), parquet)).build();
    try (writer) {
      GenericRecord row = GenericRecord.create(table.schema());
      row.setField("id", 1L);
      row.setField("message", "one");
      writer.write(row);
    }
    return writer.toDataFile();
  }

  private static DeleteFile equality(Table table) throws Exception {
    Schema schema = table.schema().select("id");
    EqualityDeleteWriter<Record> writer = Parquet.writeDeletes(table.io().newOutputFile(location(table)))
        .rowSchema(schema).withSpec(table.spec()).equalityFieldIds(schema.findField("id").fieldId())
        .createWriterFunc(parquet -> GenericParquetWriter.create(schema, parquet)).buildEqualityWriter();
    try (writer) {
      GenericRecord row = GenericRecord.create(schema);
      row.setField("id", 1L);
      writer.write(row);
    }
    return writer.toDeleteFile();
  }

  private static String location(Table table) {
    return table.location() + "/objects/" + UUID.randomUUID() + ".parquet";
  }

  private static void rejected(Table table, Runnable operation) {
    String before = ((BaseTable) table).operations().current().metadataFileLocation();
    try {
      operation.run();
      throw new AssertionError("wrong selected file use was accepted");
    } catch (BadRequestException expected) {
      table.refresh();
      require(before.equals(((BaseTable) table).operations().current().metadataFileLocation()),
          "rejection must preserve the exact selected metadata file");
    }
  }

  private static void require(boolean valid, String message) {
    if (!valid) {
      throw new AssertionError(message);
    }
  }
}
