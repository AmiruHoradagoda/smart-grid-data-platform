import csv
import random
import time

from datetime import date, timedelta
from pathlib import Path
from utils.config_loader import Config

NUMBER_OF_HOUSEHOLDS = Config.get(
    "smart_meter.number_of_households"
)

RANDOM_SEED = Config.get(
    "tariff.random_seed"
)

SIMULATED_DAY_SECONDS = Config.get(
    "simulation.simulated_day_seconds"
)

START_DATE = date.fromisoformat(
    Config.get("simulation.start_date")
)

OUTPUT_DIRECTORY = Path(
    Config.get("tariff.output_directory")
)

TARIFF_TIERS = Config.get(
    "tariff.tiers"
)


def build_tariff_profiles():
    """
    Create reproducible tariff profiles for all households.
    """

    rng = random.Random(RANDOM_SEED)

    tiers = (
        ["SUBSIDIZED"] * 4
        + ["STANDARD"] * 12
        + ["HIGH_USAGE"] * 4
    )

    rng.shuffle(tiers)

    profiles = []

    for index, tier in enumerate(
        tiers,
        start=1,
    ):

        household_id = f"HH-{index:03d}"

        profiles.append(
            {
                "household_id": household_id,
                "billing_tier": tier,
                "tariff_rate": TARIFFS[tier],
                "subsidy_flag":
                    tier == "SUBSIDIZED",
            }
        )

    return profiles

def generate_tariff_file(
    profiles,
    effective_date,
):
    """
    Write one tariff CSV for a simulated day.
    """

    OUTPUT_DIRECTORY.mkdir(
        parents=True,
        exist_ok=True,
    )

    file_path = OUTPUT_DIRECTORY / (
        f"tariffs_{effective_date.isoformat()}.csv"
    )

    with file_path.open(
        "w",
        newline="",
        encoding="utf-8",
    ) as file:

        writer = csv.DictWriter(
            file,
            fieldnames=[
                "household_id",
                "tariff_rate",
                "billing_tier",
                "subsidy_flag",
                "effective_date",
            ],
        )

        writer.writeheader()

        for profile in profiles:

            writer.writerow(
                {
                    **profile,
                    "effective_date":
                        effective_date.isoformat(),
                }
            )

    return file_path


def main():

    profiles = build_tariff_profiles()

    simulated_date = START_DATE

    print("Daily Tariff Generator Started")
    print("--------------------------------")
    print(f"Households: {len(profiles)}")
    print(
        "Simulation: 5 real minutes = "
        "1 simulated day"
    )
    print("--------------------------------")

    try:

        while True:

            file_path = generate_tariff_file(
                profiles,
                simulated_date,
            )

            print(
                f"[{simulated_date}] "
                f"Generated {file_path}"
            )

            simulated_date += timedelta(days=1)

            time.sleep(
                SIMULATED_DAY_SECONDS
            )

    except KeyboardInterrupt:

        print(
            "\nStopping tariff generator..."
        )


if __name__ == "__main__":
    main()