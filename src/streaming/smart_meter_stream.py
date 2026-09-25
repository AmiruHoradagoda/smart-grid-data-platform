from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    from_json,
    greatest,
    lit,
    round,
    sum,
    to_timestamp,
    when,
    window,
)
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
def prepare_meter_events(events_df):
    """
    Convert timestamps, validate readings,
    and calculate grid electricity import.
    """

    prepared_df = (
        events_df
        .withColumn(
            "event_timestamp",
            to_timestamp(col("timestamp")),
        )
    )

    valid_df = prepared_df.filter(
        col("event_id").isNotNull()
        & col("household_id").isNotNull()
        & col("meter_id").isNotNull()
        & col("grid_zone").isNotNull()
        & col("event_timestamp").isNotNull()
        & (col("power_consumption_kwh") >= 0)
        & (col("solar_generation_kwh") >= 0)
    )

    return valid_df.withColumn(
        "grid_import_kwh",
        greatest(
            col("power_consumption_kwh")
            - col("solar_generation_kwh"),
            lit(0.0),
        ),
    )


def calculate_zone_metrics(events_df):
    """
    Aggregate smart meter readings by grid zone
    using 5-minute simulated-time windows.
    """

    return (
        events_df
        .withWatermark(
            "event_timestamp",
            "10 minutes",
        )
        .groupBy(
            window(
                col("event_timestamp"),
                "5 minutes",
            ),
            col("grid_zone"),
        )
        .agg(
            round(
                sum("power_consumption_kwh"),
                3,
            ).alias("total_consumption_kwh"),

            round(
                sum("solar_generation_kwh"),
                3,
            ).alias("total_solar_generation_kwh"),

            round(
                sum("grid_import_kwh"),
                3,
            ).alias("total_grid_import_kwh"),

            count("household_id").alias(
                "meter_readings"
            ),
        )
        .withColumn(
            "renewable_contribution_pct",
            when(
                col("total_consumption_kwh") > 0,
                round(
                    (
                        col("total_solar_generation_kwh")
                        / col("total_consumption_kwh")
                    )
                    * 100,
                    2,
                ),
            ).otherwise(0.0),
        )
        .select(
            col("window.start").alias(
                "window_start"
            ),
            col("window.end").alias(
                "window_end"
            ),
            "grid_zone",
            "total_consumption_kwh",
            "total_solar_generation_kwh",
            "total_grid_import_kwh",
            "renewable_contribution_pct",
            "meter_readings",
        )
    )


def main():
    spark = create_spark_session()

    spark.sparkContext.setLogLevel("WARN")

    kafka_df = read_kafka_stream(spark)

    parsed_events = parse_meter_events(
        kafka_df
    )

    valid_events = prepare_meter_events(
        parsed_events
    )

    zone_metrics = calculate_zone_metrics(
        valid_events
    )

    query = (
        zone_metrics.writeStream
        .format("console")
        .outputMode("append")
        .option("truncate", False)
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()