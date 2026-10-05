// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

public final class FlinkAcceptance {
  private static String literal(String value) { return "'" + value.replace("'", "''") + "'"; }

  private static void rows(TableEnvironment environment, List<Long> expected) throws Exception {
    List<Long> actual = new ArrayList<>();
    try (CloseableIterator<Row> iterator = environment.executeSql("SELECT id, amount FROM crowdb.ecosystem.handoff").collect()) {
      while (iterator.hasNext()) {
        Row row = iterator.next();
        long id = (Long) row.getField(0);
        if ((Long) row.getField(1) != id * 10) throw new AssertionError(row);
        actual.add(id);
      }
    }
    Collections.sort(actual);
    if (!actual.equals(expected)) throw new AssertionError(actual);
  }

  public static void main(String[] args) throws Exception {
    String flink = org.apache.flink.runtime.util.EnvironmentInformation.getVersion();
    String iceberg = org.apache.iceberg.IcebergBuild.version();
    if (!flink.equals("1.20.2") || !iceberg.equals("1.11.0"))
      throw new AssertionError("Unexpected runtime: " + flink + " / " + iceberg);
    TableEnvironment environment = TableEnvironment.create(EnvironmentSettings.inBatchMode());
    environment.getConfig().set("parallelism.default", "2");
    environment.executeSql("CREATE CATALOG crowdb WITH ('type'='iceberg', 'catalog-type'='rest', "
        + "'uri'=" + literal(System.getenv("CROWDB_PREVIEW_ICEBERG_URI")) + ", "
        + "'token'=" + literal(System.getenv("ICEBERG_TOKEN")) + ", "
        + "'io-impl'='org.apache.iceberg.aws.s3.S3FileIO', 'client.region'='us-east-1', "
        + "'rest-metrics-reporting-enabled'='false', 'cache-enabled'='false')");
    if (args[0].equals("mutate")) {
      rows(environment, List.of(1L, 3L, 4L, 5L));
      environment.executeSql("INSERT INTO crowdb.ecosystem.handoff (id, amount, note) VALUES (CAST(6 AS BIGINT), CAST(60 AS BIGINT), CAST(NULL AS STRING))").await();
    } else if (!args[0].equals("verify")) throw new IllegalArgumentException(args[0]);
    rows(environment, List.of(1L, 3L, 4L, 5L, 6L));
    System.out.println("Flink 1.20.2 / Iceberg 1.11.0 " + args[0] + " passed");
  }
}
