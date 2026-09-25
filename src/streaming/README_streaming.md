# Smart Meter Streaming

This folder contains the Spark Structured Streaming job for the Smart Grid Data Platform.

## Purpose

`smart_meter_stream.py` reads smart meter events from Kafka, converts the JSON messages into structured Spark columns, and prepares the data for later real-time processing.

Current flow:

```text
Smart Meter Producer
        ↓
      Kafka
smart-meter-readings
        ↓
Spark Structured Streaming
        ↓
Parse JSON
        ↓
Structured meter events
        ↓
Console output
```

## Kafka Source

Spark reads from:

```text
Topic: smart-meter-readings
Broker inside Docker: kafka:29092
```

Kafka initially provides fields such as:

```text
key
value
partition
offset
timestamp
```

The actual smart meter JSON is inside the Kafka `value` field.

## Event Parsing

The stream converts the Kafka value from binary data to text and parses it using a fixed Spark schema.

Expected event fields:

```text
event_id
schema_version
meter_id
household_id
power_consumption_kwh
solar_generation_kwh
grid_zone
timestamp
```

Using an explicit schema helps keep the streaming data consistent and avoids automatic schema guessing.

## Kafka Metadata

The script also keeps:

```text
kafka_key
partition
offset
```

These fields are useful later for debugging, tracing, and monitoring.

## Current Output

At this stage, the processed events are written to the Spark console.

This is mainly used to verify that:

- Kafka and Spark are connected correctly
- JSON messages are parsed correctly
- expected smart meter fields are available

## Next Steps

Later this stream will be extended to:

```text
convert event timestamps
validate readings
calculate grid import
apply event-time windows
aggregate by grid zone
calculate renewable contribution
store processed results
```

## Note

Spark runs inside Docker, so it connects to Kafka using:

```text
kafka:29092
```

If Spark is run directly from the host machine instead, the Kafka address would normally be:

```text
localhost:9092
```
