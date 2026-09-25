from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
)


KAFKA_BOOTSTRAP_SERVERS = "kafka:29092"
KAFKA_TOPIC = "smart-meter-readings"


SMART_METER_SCHEMA = StructType([
    StructField("event_id", StringType(), False),
    StructField("schema_version", IntegerType(), False),

    StructField("meter_id", StringType(), False),
    StructField("household_id", StringType(), False),

    StructField("power_consumption_kwh", DoubleType(), False),
    StructField("solar_generation_kwh", DoubleType(), False),

    StructField("grid_zone", StringType(), False),
    StructField("timestamp", StringType(), False),
])


def create_spark_session():
    return (
        SparkSession.builder
        .appName("SmartGridMeterStreaming")
        .getOrCreate()
    )


def read_kafka_stream(spark):
    return (
        spark.readStream
        .format("kafka")
        .option(
            "kafka.bootstrap.servers",
            KAFKA_BOOTSTRAP_SERVERS,
        )
        .option(
            "subscribe",
            KAFKA_TOPIC,
        )
        .option(
            "startingOffsets",
            "latest",
        )
        .load()
    )


def parse_meter_events(kafka_df):
    return (
        kafka_df
        .select(
            col("key").cast("string").alias("kafka_key"),
            col("value").cast("string").alias("json_value"),
            col("partition"),
            col("offset"),
        )
        .withColumn(
            "data",
            from_json(
                col("json_value"),
                SMART_METER_SCHEMA,
            ),
        )
        .select(
            "kafka_key",
            "partition",
            "offset",
            "data.*",
        )
    )


def main():
    spark = create_spark_session()

    spark.sparkContext.setLogLevel("WARN")

    kafka_df = read_kafka_stream(spark)

    meter_events = parse_meter_events(
        kafka_df
    )

    query = (
        meter_events.writeStream
        .format("console")
        .outputMode("append")
        .option("truncate", False)
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()