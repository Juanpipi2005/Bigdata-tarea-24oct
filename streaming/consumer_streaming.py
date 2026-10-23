"""Structured Streaming: AFR por modelo en ventanas de 7 días (watermark 2 días).

Uso:
  spark-submit --packages $SPARK_KAFKA_PACKAGE streaming/consumer_streaming.py            # consola
  spark-submit --packages $SPARK_KAFKA_PACKAGE streaming/consumer_streaming.py mongo      # Parte D
"""
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = "discos-eventos"
CHECKPOINT = "/tmp/chk"

schema = StructType([
    StructField("date", StringType()),
    StructField("serial_number", StringType()),
    StructField("model", StringType()),
    StructField("failure", IntegerType()),
])


def build_afr_stream(spark):
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )
    events = (
        raw.select(F.from_json(F.col("value").cast("string"), schema).alias("e"))
        .select("e.*")
        .withColumn("event_time", F.to_timestamp("date", "yyyy-MM-dd"))
    )
    return (
        events.withWatermark("event_time", "2 days")
        .groupBy(F.window("event_time", "7 days"), "model")
        .agg(
            F.count("*").alias("dias_disco"),
            F.sum("failure").alias("fallas"),
        )
        .withColumn("afr", F.col("fallas") / (F.col("dias_disco") / 365) * 100)
        .select(
            F.col("window.start").alias("ventana_inicio"),
            F.col("window.end").alias("ventana_fin"),
            "model", "dias_disco", "fallas", "afr",
        )
    )


def write_console(afr):
    return (
        afr.writeStream.format("console")
        .outputMode("update")
        .option("truncate", "false")
        .option("numRows", 50)
        .option("checkpointLocation", CHECKPOINT)
        .start()
    )


# --- Parte D (opcional): MongoDB vía foreachBatch --------------------------------
def write_to_mongo(batch_df, batch_id):
    """Upsert por (ventana_inicio, model) en discos.agregados_afr.

    En outputMode update cada batch trae solo las ventanas modificadas, por eso upsert.
    """
    from pymongo import MongoClient, UpdateOne

    rows = batch_df.collect()
    if not rows:
        return
    ops = [
        UpdateOne(
            {"ventana_inicio": r["ventana_inicio"], "model": r["model"]},
            {"$set": r.asDict()},
            upsert=True,
        )
        for r in rows
    ]
    client = MongoClient(os.environ.get("MONGO_URI", "mongodb://mongo:27017"))
    try:
        client["discos"]["agregados_afr"].bulk_write(ops)
    finally:
        client.close()


def write_mongo(afr):
    return (
        afr.writeStream.outputMode("update")
        .foreachBatch(write_to_mongo)
        .option("checkpointLocation", CHECKPOINT + "_mongo")
        .start()
    )


if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("afr-streaming")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    afr = build_afr_stream(spark)
    query = write_mongo(afr) if "mongo" in sys.argv[1:] else write_console(afr)
    query.awaitTermination()
