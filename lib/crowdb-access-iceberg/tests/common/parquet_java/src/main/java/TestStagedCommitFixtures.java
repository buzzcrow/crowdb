package org.apache.iceberg.rest;

import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.MetadataUpdateParser;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.UpdateRequirements;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.types.Types;

public final class TestStagedCommitFixtures {
  public static void main(String[] args) throws Exception {
    Method createChanges = RESTSessionCatalog.class.getDeclaredMethod("createChanges", TableMetadata.class);
    createChanges.setAccessible(true);
    Schema schema = new Schema(Types.NestedField.required(91, "id", Types.LongType.get()));
    for (int version = 1; version <= 3; version++) {
      TableMetadata draft = TableMetadata.newTableMetadata(schema, PartitionSpec.unpartitioned(), args[0],
          Map.of("format-version", String.valueOf(version)));
      List<MetadataUpdate> updates = new ArrayList<>();
      for (Object update : (List<?>) createChanges.invoke(null, draft)) {
        updates.add((MetadataUpdate) update);
      }
      updates.add(new MetadataUpdate.SetProperties(Map.of("transaction", "committed")));
      long timestamp = System.currentTimeMillis();
      String snapshot = "{\"action\":\"add-snapshot\",\"snapshot\":{\"snapshot-id\":10,"
          + (version == 1 ? "" : "\"sequence-number\":1,")
          + "\"timestamp-ms\":" + timestamp + ",\"schema-id\":0,\"summary\":{\"operation\":\"append\"},"
          + "\"manifest-list\":\"" + args[0] + "metadata/snapshot.avro\""
          + (version == 3 ? ",\"first-row-id\":0,\"added-rows\":2" : "") + "}}";
      updates.add(MetadataUpdateParser.fromJson(snapshot));
      updates.add(MetadataUpdateParser.fromJson("{\"action\":\"set-snapshot-ref\","
          + "\"ref-name\":\"main\",\"type\":\"branch\",\"snapshot-id\":10}"));
      TableMetadata.Builder builder = TableMetadata.buildFromEmpty(version);
      for (MetadataUpdate update : updates) {
        update.applyTo(builder);
      }
      TableMetadata result = builder.build();
      String output = TableMetadataParser.toJson(result);
      TableMetadataParser.fromJson(output);
      UpdateTableRequest request = new UpdateTableRequest(UpdateRequirements.forCreateTable(updates), updates);
      emit("REQUEST_V" + version, RESTObjectMapper.mapper().writeValueAsString(request));
      emit("OUTPUT_V" + version, output);
    }
  }

  private static void emit(String name, String value) {
    System.out.println(name + "=" + Base64.getEncoder().encodeToString(value.getBytes(StandardCharsets.UTF_8)));
  }
}
