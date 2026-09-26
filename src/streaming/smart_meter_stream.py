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
from pyspark.sql import functions as F
from utils.config_loader import Config

POSTGRES_URL = (
    f"jdbc:postgresql://{Config.get('postgres.docker.hostname')}:"
    f"{Config.get('postgres.docker.port')}/{Config.get('postgres.database')}"
)
POSTGRES_PROPERTIES = {
    "user": Config.get("postgres.user"),
    "password": Config.get("postgres.password"),
    "driver": "org.postgresql.Driver",
}


KAFKA_BOOTSTRAP_SERVERS = Config.get(
    "kafka.bootstrap_servers.docker"
)
KAFKA_TOPIC = Config.get(
    "kafka.topic"
)
WINDOW_DURATION = Config.get(
    "spark.window_duration"
)

WATERMARK_DURATION = Config.get(
    "spark.watermark_duration"
)
HOUSEHOLD_CHECKPOINT_LOCATION = Config.get(
    "spark.household_checkpoint_location"
)
ZONE_CHECKPOINT_LOCATION = Config.get(
    "spark.zone_checkpoint_location"
)

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
        .appName(Config.get("spark.application_name"))
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
            Config.get("spark.starting_offsets"),
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
            WATERMARK_DURATION,
        )
        .groupBy(
            window(
                col("event_timestamp"),
                WINDOW_DURATION,
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
def write_zone_metrics_to_postgres(batch_df, batch_id):
    """
    Write finalized zone metrics from one Spark micro-batch
    into PostgreSQL.
    """

    if batch_df.isEmpty():
        return

    (
        batch_df.write
        .jdbc(
            url=POSTGRES_URL,
            table=Config.get("postgres.tables.zone_energy_metrics"),
            mode="append",
            properties=POSTGRES_PROPERTIES,
        )
    )

    print(
        f"Stored PostgreSQL batch: {batch_id}"
    )
def calculate_daily_household_energy(clean_df):
    """
    Calculate one daily energy summary for each household.
    """

    return (
        clean_df
        .withWatermark(
            "event_timestamp",
            Config.get("spark.watermark_duration"),
        )
        .groupBy(
            F.window(
                F.col("event_timestamp"),
                "1 day",
            ),
            F.col("household_id"),
            F.col("grid_zone"),
        )
        .agg(
            F.round(
                F.sum("power_consumption_kwh"),
                3,
            ).alias("total_consumption_kwh"),

            F.round(
                F.sum("solar_generation_kwh"),
                3,
            ).alias("total_solar_generation_kwh"),

            F.round(
                F.sum("grid_import_kwh"),
                3,
            ).alias("total_grid_import_kwh"),

            F.count("*").alias(
                "meter_readings"
            ),
        )
        .select(
            F.to_date(
                F.col("window.start")
            ).alias("energy_date"),

            "household_id",
            "grid_zone",
            "total_consumption_kwh",
            "total_solar_generation_kwh",
            "total_grid_import_kwh",
            "meter_readings",
        )
    )
def write_household_batch(batch_df, batch_id):
    """
    Store finalized daily household energy totals
    in PostgreSQL.
    """

    if batch_df.isEmpty():
        return

    (
        batch_df.write
        .jdbc(
            url=POSTGRES_URL,
            table="household_daily_energy",
            mode="append",
            properties=POSTGRES_PROPERTIES,
        )
    )

    print(
        f"Stored household batch: {batch_id}"
    )
    
def main():
    spark = create_spark_session()

    spark.sparkContext.setLogLevel("WARN")

    # Read continuous smart-meter events from Kafka.
    kafka_df = read_kafka_stream(spark)

    # Parse JSON events.
    parsed_events = parse_meter_events(
        kafka_df
    )

    # Validate events and calculate grid import.
    valid_events = prepare_meter_events(
        parsed_events
    )

    # -------------------------------------------------
    # Stream 1: Real-time zone metrics
    # -------------------------------------------------

    zone_metrics = calculate_zone_metrics(
        valid_events
    )

    zone_query = (
        zone_metrics.writeStream
        .outputMode("append")
        .foreachBatch(
            write_zone_metrics_to_postgres
        )
        .option(
            "checkpointLocation",
            ZONE_CHECKPOINT_LOCATION,
        )
        .start()
    )

    # -------------------------------------------------
    # Stream 2: Daily household energy
    # -------------------------------------------------

    household_daily_energy = (
        calculate_daily_household_energy(
            valid_events
        )
    )

    household_query = (
        household_daily_energy.writeStream
        .outputMode("append")
        .foreachBatch(
            write_household_batch
        )
        .option(
            "checkpointLocation",
            HOUSEHOLD_CHECKPOINT_LOCATION,
        )
        .start()
    )

    # Keep both streaming queries running.
    spark.streams.awaitAnyTermination()
if __name__ == "__main__":
    main()
