import java.util.Base64;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageTypeParser;

public final class TestNullableParquetFixtures {
  public static void main(String[] args) throws Exception {
    var schema = MessageTypeParser.parseMessageType(
        "message nullable { optional group parent { optional int32 value = 2; } }");
    var factory = new SimpleGroupFactory(schema);
    for (var version : ParquetProperties.WriterVersion.values()) {
      for (boolean allNull : new boolean[] {false, true}) {
        var file = java.nio.file.Files.createTempFile("nullable-parquet-", ".parquet");
        try {
          try (var writer = ExampleParquetWriter.builder(new Path(file.toUri()))
              .withType(schema).withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
              .withWriterVersion(version).withCompressionCodec(CompressionCodecName.ZSTD).build()) {
            for (int index = 0; index < 64; index++) {
              var row = factory.newGroup();
              if (index % 4 != 0) {
                var parent = row.addGroup("parent");
                if (!allNull && index % 4 >= 2) {
                  parent.append("value", index % 4 == 2 ? 11 : 22);
                }
              }
              writer.write(row);
            }
          }
          System.out.println("FIXTURE " + version.name() + "_" + allNull + "="
              + Base64.getEncoder().encodeToString(java.nio.file.Files.readAllBytes(file)));
        } finally {
          java.nio.file.Files.deleteIfExists(file);
        }
      }
    }
  }
}
