// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

import java.util.List;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public final class SparkAcceptance {
  private static void rows(SparkSession spark, String table, List<Long> expected) {
    List<Row> actual = spark.sql("SELECT id, amount FROM " + table + " ORDER BY id").collectAsList();
    if (actual.size() != expected.size()) throw new AssertionError(actual);
    for (int index = 0; index < expected.size(); index++) {
      long id = expected.get(index);
      if (actual.get(index).getLong(0) != id || actual.get(index).getLong(1) != 10 * id)
        throw new AssertionError(actual);
    }
  }

  public static void main(String[] args) {
    SparkSession spark = SparkSession.builder().master("local[2]").appName("crowdb-ecosystem")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.crowdb", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.crowdb.type", "rest")
        .config("spark.sql.catalog.crowdb.uri", System.getenv("CROWDB_PREVIEW_ICEBERG_URI"))
        .config("spark.sql.catalog.crowdb.token", System.getenv("ICEBERG_TOKEN"))
        .config("spark.sql.catalog.crowdb.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.crowdb.client.region", "us-east-1")
        .config("spark.sql.catalog.crowdb.rest-metrics-reporting-enabled", "false")
        .getOrCreate();
    try {
      if (!spark.version().equals("3.5.6")) throw new AssertionError(spark.version());
      String iceberg = org.apache.iceberg.IcebergBuild.version();
      if (!iceberg.equals("1.11.0")) throw new AssertionError(iceberg);
      if (args[0].equals("mutate")) {
        rows(spark, "crowdb.ecosystem.handoff", List.of(1L, 2L, 3L, 4L));
        spark.sql("ALTER TABLE crowdb.ecosystem.handoff ADD COLUMN note STRING");
        spark.sql("ALTER TABLE crowdb.ecosystem.handoff ADD PARTITION FIELD bucket(2, id)");
        spark.sql("ALTER TABLE crowdb.ecosystem.handoff SET TBLPROPERTIES ('write.delete.mode'='merge-on-read')");
        spark.sql("INSERT INTO crowdb.ecosystem.handoff (id, amount, note) VALUES (5, 50, CAST(NULL AS STRING))");
        spark.sql("DELETE FROM crowdb.ecosystem.handoff WHERE id = 2");
        rows(spark, "crowdb.ecosystem.handoff", List.of(1L, 3L, 4L, 5L));
        spark.sql("CREATE TABLE crowdb.ecosystem.lifecycle (id BIGINT, amount BIGINT) USING iceberg TBLPROPERTIES ('format-version'='2')");
        spark.sql("INSERT INTO crowdb.ecosystem.lifecycle VALUES (9, 90)");
        spark.sql("ALTER TABLE crowdb.ecosystem.lifecycle RENAME TO renamed");
        rows(spark, "crowdb.ecosystem.renamed", List.of(9L));
        spark.sql("DROP TABLE crowdb.ecosystem.renamed");
      } else if (args[0].equals("verify")) {
        rows(spark, "crowdb.ecosystem.handoff", List.of(1L, 3L, 4L, 5L, 6L));
        rows(spark, "crowdb.ecosystem.handoff VERSION AS OF " + args[1], List.of(1L, 3L, 4L, 5L, 6L));
        Row aggregate = spark.sql("SELECT count(*), sum(amount) FROM crowdb.ecosystem.handoff").first();
        if (aggregate.getLong(0) != 5 || aggregate.getLong(1) != 190) throw new AssertionError(aggregate);
      } else throw new IllegalArgumentException(args[0]);
      System.out.println("Spark 3.5.6 / Iceberg 1.11.0 " + args[0] + " passed");
    } finally {
      spark.stop();
    }
  }
}
