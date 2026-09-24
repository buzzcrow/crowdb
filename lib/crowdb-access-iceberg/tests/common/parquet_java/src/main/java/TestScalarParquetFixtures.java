import java.util.Arrays;
import java.util.Base64;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageTypeParser;

public final class TestScalarParquetFixtures {
  public static void main(String[] args) throws Exception {
    var schema = MessageTypeParser.parseMessageType("message scalars {"
        + " optional boolean flag = 1; optional float single = 2; optional double real = 3;"
        + " optional fixed_len_byte_array(16) fixed = 4; optional binary bytes = 5; }");
    var factory = new SimpleGroupFactory(schema);
    float[] singles = {0, -0.0f, Float.intBitsToFloat(0x7fc01234), Float.POSITIVE_INFINITY,
        Float.NEGATIVE_INFINITY, 1.5f, -3.25f, Float.MIN_VALUE};
    double[] doubles = {0, -0.0d, Double.longBitsToDouble(0x7ff8000000001234L), Double.POSITIVE_INFINITY,
        Double.NEGATIVE_INFINITY, 1.5d, -3.25d, Double.MIN_VALUE};
    for (var version : ParquetProperties.WriterVersion.values()) {
      for (boolean dictionary : new boolean[] {false, true}) {
        var file = java.nio.file.Files.createTempFile("scalar-parquet-", ".parquet");
        try {
          try (var writer = ExampleParquetWriter.builder(new Path(file.toUri()))
              .withType(schema).withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
              .withWriterVersion(version).withDictionaryEncoding(dictionary)
              .withCompressionCodec(CompressionCodecName.ZSTD).build()) {
            for (int index = 0; index < 8; index++) {
              var row = factory.newGroup();
              if (index > 0) {
                var fixed = new byte[16];
                var bytes = new byte[2048];
                Arrays.fill(fixed, (byte) index);
                Arrays.fill(bytes, (byte) index);
                row.append("flag", index % 2 == 1).append("single", singles[index])
                    .append("real", doubles[index]).append("fixed", Binary.fromConstantByteArray(fixed))
                    .append("bytes", Binary.fromConstantByteArray(bytes));
              }
              writer.write(row);
            }
          }
          System.out.println("FIXTURE " + version.name() + "_" + dictionary + "="
              + Base64.getEncoder().encodeToString(java.nio.file.Files.readAllBytes(file)));
        } finally {
          java.nio.file.Files.deleteIfExists(file);
        }
      }
    }
  }
}
