#!/usr/bin/env python3
"""crypto-medallion streaming job (design §5.3).

One always-on Dataproc Spark job on the GKE-virtual cluster, attached to the
Hive Metastore. It reads the Managed Kafka `trades` topic once and fans it out
to the three medallion layers:

  bronze  foreachBatch -> gs://.../bronze/trades   raw trade JSON, as received
  silver  foreachBatch -> gs://.../silver/trades   parsed, typed, deduped
                         -> gs://.../silver/quarantine  malformed rows
  gold    windowed       -> Iceberg table on the HMS catalog
          live aggregate -> gs://.../leaderboard/latest.json  (serving path)

The gold/live split is deliberate: the Iceberg table carries the per-minute
OHLC windows (design §6), while `latest.json` is a per-symbol running aggregate
so the leaderboard moves every trigger instead of once per window.
"""

from __future__ import annotations

import argparse
import json
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (DoubleType, LongType, StringType, StructField,
                               StructType)

TRADE_SCHEMA = StructType([
    StructField("symbol", StringType()),
    StructField("price", DoubleType()),
    StructField("size", DoubleType()),
    StructField("ts", LongType()),
    StructField("id", StringType()),
])


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", required=True)
    p.add_argument("--topic", required=True)
    p.add_argument("--bronze", required=True)
    p.add_argument("--silver", required=True)
    p.add_argument("--quarantine", required=True)
    p.add_argument("--leaderboard", required=True)
    p.add_argument("--iceberg-db", required=True)
    p.add_argument("--iceberg-table", required=True)
    p.add_argument("--group", required=True)
    return p.parse_args()


ARGS = parse_args()
spark = SparkSession.builder.appName("crypto-medallion").getOrCreate()
spark.sparkContext.setLogLevel("WARN")


def write_gcs_text(uri: str, text: str) -> None:
    """Write one object to a gs:// path through the wired GCS connector."""
    jvm = spark._jvm
    conf = spark._jsc.hadoopConfiguration()
    path = jvm.org.apache.hadoop.fs.Path(uri)
    fs = path.getFileSystem(conf)
    out = fs.create(path, True)
    out.write(bytearray(text.encode("utf-8")))
    out.close()


# ── gold table (Iceberg on the HMS catalog) ──────────────────────────────────
gold_table = f"hms.{ARGS.iceberg_db}.{ARGS.iceberg_table}"
spark.sql(f"CREATE DATABASE IF NOT EXISTS hms.{ARGS.iceberg_db}")
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {gold_table} (
  symbol string,
  window_start timestamp,
  window_end timestamp,
  open double,
  high double,
  low double,
  close double,
  vwap double,
  volume double,
  trades bigint
) USING iceberg
""")
print(f"GOLD_TABLE={gold_table}", flush=True)

# ── source: one Kafka read, three writers ────────────────────────────────────
raw = (spark.readStream.format("kafka")
       .option("kafka.bootstrap.servers", ARGS.bootstrap)
       .option("subscribe", ARGS.topic)
       .option("startingOffsets", "earliest")
       .option("kafka.group.id", ARGS.group)
       .option("failOnDataLoss", "false")
       .load())

parsed = (raw
          .selectExpr("CAST(value AS STRING) AS raw")
          .select(F.from_json("raw", TRADE_SCHEMA).alias("t"), "raw")
          .select("t.*", "raw")
          .withColumn("event_ts", (F.col("ts") / 1000).cast("timestamp")))
valid = parsed.filter(
    F.col("symbol").isNotNull() & F.col("price").isNotNull()
    & F.col("size").isNotNull() & F.col("event_ts").isNotNull())
malformed = parsed.filter(
    F.col("symbol").isNull() | F.col("price").isNull() | F.col("event_ts").isNull())


# ── bronze + silver (foreachBatch) ───────────────────────────────────────────
def bronze_silver_batch(batch_df, epoch_id):
    batch_df.persist()
    raw_rows = batch_df.select(F.col("raw").alias("value"))
    raw_rows.write.mode("append").text(ARGS.bronze)

    good = (batch_df.filter(F.col("symbol").isNotNull() & F.col("price").isNotNull())
            .dropDuplicates(["id"])
            .select("symbol", "price", "size", "event_ts", "id"))
    good.write.mode("append").parquet(ARGS.silver)

    bad = batch_df.filter(F.col("symbol").isNull() | F.col("price").isNull())
    if bad.take(1):
        bad.select("raw").write.mode("append").text(ARGS.quarantine)
    print(f"SILVER_BATCH epoch={epoch_id} rows={batch_df.count()} "
          f"parsed={good.count()} quarantined={bad.count()}", flush=True)
    batch_df.unpersist()


bronze_silver = (parsed.writeStream
                 .foreachBatch(bronze_silver_batch)
                 .option("checkpointLocation", f"{ARGS.bronze}/_checkpoint")
                 .trigger(processingTime="5 seconds")
                 .start())


# ── gold: per-minute OHLC windows -> Iceberg + leaderboard/latest.json ───────
# One windowed query feeds both the Iceberg table and the serving rollup, so
# only a single stateful aggregation runs on the memory-tight k3d node.
def gold_batch(batch_df, epoch_id):
    out = (batch_df
           .select(F.col("window.start").alias("window_start"),
                   F.col("window.end").alias("window_end"),
                   "symbol", "open", "high", "low", "close", "vwap", "volume", "trades")
           .withColumn("trades", F.col("trades").cast("bigint")))
    out.writeTo(gold_table).append()

    now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    def iso(ts):
        # Firestore timestampValue needs an RFC3339 offset; Spark timestamps are UTC.
        return ts.strftime("%Y-%m-%dT%H:%M:%SZ") if ts else None
    rows = out.orderBy(F.col("volume").desc()).collect()
    lines = []
    for r in rows:
        lines.append(json.dumps({
            "symbol": r["symbol"],
            "vwap": round(float(r["vwap"] or 0.0), 4),
            "volume": round(float(r["volume"] or 0.0), 6),
            "trades": int(r["trades"] or 0),
            "window_start": iso(r["window_start"]),
            "window_end": iso(r["window_end"]),
            "updated_at": now,
        }, separators=(",", ":")))
    if lines:
        write_gcs_text(ARGS.leaderboard, "\n".join(lines) + "\n")
    print(f"GOLD_BATCH epoch={epoch_id} windows={len(rows)}", flush=True)


gold = (valid
        .withWatermark("event_ts", "5 seconds")
        .groupBy(F.window("event_ts", "10 seconds"), "symbol")
        .agg(F.first("price").alias("open"),
             F.max("price").alias("high"),
             F.min("price").alias("low"),
             F.last("price").alias("close"),
             (F.sum(F.col("price") * F.col("size")) / F.sum("size")).alias("vwap"),
             F.sum("size").alias("volume"),
             F.count("*").alias("trades")))

gold_query = (gold.writeStream
              .foreachBatch(gold_batch)
              .outputMode("append")
              .option("checkpointLocation", f"{ARGS.iceberg_db}/gold/_checkpoint")
              .trigger(processingTime="5 seconds")
              .start())

print("MEDALLION_RUNNING", flush=True)
spark.streams.awaitAnyTermination()
