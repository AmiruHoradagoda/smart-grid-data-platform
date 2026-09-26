import json
import math
import random
import time
import uuid

from datetime import datetime, timedelta, timezone
from confluent_kafka import Producer
from utils.config_loader import Config


KAFKA_TOPIC = Config.get(
    "kafka.topic"
)

KAFKA_BOOTSTRAP_SERVERS = Config.get(
    "kafka.bootstrap_servers.host"
)

NUMBER_OF_HOUSEHOLDS = Config.get(
    "smart_meter.number_of_households"
)

NUMBER_OF_SOLAR_HOUSES = Config.get(
    "smart_meter.number_of_solar_houses"
)

RANDOM_SEED = Config.get(
    "smart_meter.random_seed"
)

EVENT_INTERVAL_SECONDS = Config.get(
    "smart_meter.event_interval_seconds"
)

GRID_ZONES = Config.get(
    "smart_meter.grid_zones"
)

rng = random.Random(RANDOM_SEED)

SOLAR_HOUSEHOLDS = set(
    rng.sample(
        range(1, NUMBER_OF_HOUSEHOLDS + 1),
        NUMBER_OF_SOLAR_HOUSES
    )
)

SIMULATION_SPEED = Config.get("simulation.simulation_speed")
REAL_START_TIME = datetime.now(timezone.utc)
SIMULATION_START_TIME = datetime.fromisoformat(
    Config.get("simulation.start_date")
).replace(tzinfo=timezone.utc)


def get_simulated_time():
    """
    Convert real elapsed time into accelerated simulated time.
    """

    real_elapsed = (
        datetime.now(timezone.utc) - REAL_START_TIME
    ).total_seconds()

    simulated_elapsed = real_elapsed * SIMULATION_SPEED

    return SIMULATION_START_TIME + timedelta(
        seconds=simulated_elapsed
    )


def calculate_consumption(hour, base_load):
    """
    Generate a realistic household electricity consumption pattern.

    Time periods:
    - 00:00 - 05:00 : Low night usage
    - 05:00 - 06:00 : Early morning
    - 06:00 - 09:00 : Morning peak
    - 09:00 - 18:00 : Normal daytime usage
    - 18:00 - 22:00 : Evening peak
    - 22:00 - 24:00 : Late evening usage
    """

    if not 0 <= hour < 24:
        raise ValueError("hour must be between 0 and 24")

    if 0 <= hour < 5:
        multiplier = Config.get("smart_meter.consumption.multipliers.midnight_to_05")

    elif 5 <= hour < 6:
        multiplier = Config.get("smart_meter.consumption.multipliers.05_to_06")

    elif 6 <= hour < 9:
        multiplier = Config.get("smart_meter.consumption.multipliers.06_to_09")

    elif 9 <= hour < 18:
        multiplier = Config.get("smart_meter.consumption.multipliers.09_to_18")

    elif 18 <= hour < 22:
        multiplier = Config.get("smart_meter.consumption.multipliers.18_to_22")

    else:  # 22:00 - 24:00
        multiplier = Config.get("smart_meter.consumption.multipliers.22_to_midnight")

    random_variation = random.uniform(
        Config.get("smart_meter.consumption.random_variation.min"),
        Config.get("smart_meter.consumption.random_variation.max"),
    )

    consumption = (
        base_load
        * multiplier
        * random_variation
    )

    return round(consumption, 3)

def calculate_solar_generation(hour, solar_capacity):
    """
    Simulate solar generation.

    Solar production:
    - Starts around 06:00
    - Peaks near midday
    - Falls to zero around 18:00
    """

    if solar_capacity == 0:
        return 0.0

    sunrise = Config.get("smart_meter.solar_generation.sunrise_hour")
    peak = Config.get("smart_meter.solar_generation.peak_hour")
    sunset = Config.get("smart_meter.solar_generation.sunset_hour")
    if hour < sunrise or hour >= sunset:
        return 0.0

    # Half a sine wave on either side of the configured peak.
    if hour <= peak:
        daylight_progress = 0.5 * (hour - sunrise) / (peak - sunrise)
    else:
        daylight_progress = 0.5 + 0.5 * (hour - peak) / (sunset - peak)

    solar_factor = math.sin(
        math.pi * daylight_progress
    )

    weather_variation = random.uniform(
        Config.get("smart_meter.solar_generation.weather_variation.min"),
        Config.get("smart_meter.solar_generation.weather_variation.max"),
    )

    generation = (
        solar_capacity
        * solar_factor
        * weather_variation
    )

    return round(max(generation, 0), 3)


def build_households():
    """
    Create household profiles.
    """

    households = []

    for index in range(1, NUMBER_OF_HOUSEHOLDS + 1):

        household_id = f"HH-{index:03d}"
        meter_id = f"METER-{index:03d}"

        grid_zone = GRID_ZONES[
            (index - 1) % len(GRID_ZONES)
        ]  # Assign households to zones in round-robin order.

        base_load = random.uniform(
            Config.get("smart_meter.base_load.min_kwh"),
            Config.get("smart_meter.base_load.max_kwh"),
        )

        has_solar = index in SOLAR_HOUSEHOLDS

        solar_capacity = (
            random.uniform(
                Config.get("smart_meter.solar_capacity.min_kwh"),
                Config.get("smart_meter.solar_capacity.max_kwh"),
            )
            if has_solar
            else 0
        )

        households.append(
            {
                "household_id": household_id,
                "meter_id": meter_id,
                "grid_zone": grid_zone,
                "base_load": base_load,
                "solar_capacity": solar_capacity,
            }
        )

    return households


def create_meter_event(household):
    simulated_time = get_simulated_time()

    # Include minutes for smoother solar variation.
    decimal_hour = (
        simulated_time.hour
        + simulated_time.minute / 60
    )

    consumption = calculate_consumption(
        decimal_hour,
        household["base_load"],
    )

    solar_generation = calculate_solar_generation(
        decimal_hour,
        household["solar_capacity"],
    )

    return {
        "event_id": str(uuid.uuid4()),
        "schema_version": 1,

        "meter_id": household["meter_id"],
        "household_id": household["household_id"],

        "power_consumption_kwh": consumption,
        "solar_generation_kwh": solar_generation,

        "grid_zone": household["grid_zone"],

        "timestamp": simulated_time.isoformat(),
    }


def delivery_callback(error, message):
    if error:
        print(
            f"Delivery failed: {error}"
        )


def main():
    producer = Producer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "client.id": "smart-meter-simulator",
        }
    )

    households = build_households()

    print("Smart Meter Simulator Started")
    print("--------------------------------")
    print(f"Households: {len(households)}")
    print(f"Kafka Topic: {KAFKA_TOPIC}")
    print(
        f"Simulation speed: {SIMULATION_SPEED} simulated seconds per real second"
    )
    print("--------------------------------")

    try:

        while True:

            for household in households:

                event = create_meter_event(
                    household
                )

                producer.produce(
                    topic=KAFKA_TOPIC,

                    key=event[
                        "household_id"
                    ],

                    value=json.dumps(
                        event
                    ),

                    callback=delivery_callback,
                )

                # Allow delivery callbacks to execute.
                producer.poll(0)

            simulated_time = get_simulated_time()

            print(
                f"[{simulated_time.isoformat()}] "
                f"Published "
                f"{len(households)} readings"
            )

            time.sleep(
                EVENT_INTERVAL_SECONDS
            )

    except KeyboardInterrupt:

        print("\nStopping simulator...")

    finally:

        producer.flush()

        print(
            "All pending Kafka messages flushed."
        )


if __name__ == "__main__":
    main()
