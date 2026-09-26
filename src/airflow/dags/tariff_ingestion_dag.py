import csv

from datetime import datetime
from pathlib import Path

import psycopg2

from airflow.sdk import dag, task

from utils.config_loader import Config


TARIFF_DIRECTORY = (
    Path("/opt/airflow")
    / Config.get("tariff.output_directory")
)

EXPECTED_COLUMNS = {
    "household_id",
    "tariff_rate",
    "billing_tier",
    "subsidy_flag",
    "effective_date",
}


def get_postgres_connection():
    return psycopg2.connect(
        host=Config.get(
            "postgres.docker.hostname"
        ),
        port=Config.get(
            "postgres.docker.port"
        ),
        dbname=Config.get(
            "postgres.database"
        ),
        user=Config.get(
            "postgres.user"
        ),
        password=Config.get(
            "postgres.password"
        ),
    )


@dag(
    schedule="*/5 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["smart-grid", "tariff"],
)
def tariff_ingestion():

    @task
    def find_tariff_files():
        """
        Find all available daily tariff CSV files.
        """

        files = sorted(
            TARIFF_DIRECTORY.glob(
                "tariffs_*.csv"
            )
        )

        if not files:
            raise FileNotFoundError(
                "No tariff CSV files found."
            )

        return [
            str(file_path)
            for file_path in files
        ]

    @task
    def validate_tariff_files(file_paths):
        """
        Validate every tariff CSV before loading.
        """

        expected_households = Config.get(
            "smart_meter.number_of_households"
        )

        valid_files = []

        for file_path in file_paths:

            path = Path(file_path)

            with path.open(
                "r",
                encoding="utf-8",
            ) as file:

                reader = csv.DictReader(file)

                if set(reader.fieldnames) != EXPECTED_COLUMNS:
                    raise ValueError(
                        f"Invalid columns in {path.name}"
                    )

                rows = list(reader)

            if len(rows) != expected_households:
                raise ValueError(
                    f"{path.name}: expected "
                    f"{expected_households} households, "
                    f"found {len(rows)}"
                )

            household_ids = set()

            for row in rows:

                household_id = row["household_id"]

                if household_id in household_ids:
                    raise ValueError(
                        f"{path.name}: duplicate household "
                        f"{household_id}"
                    )

                household_ids.add(
                    household_id
                )

                tariff_rate = float(
                    row["tariff_rate"]
                )

                if tariff_rate < 0:
                    raise ValueError(
                        f"{path.name}: tariff rate "
                        "cannot be negative"
                    )

            valid_files.append(
                str(path)
            )

        return valid_files

    @task
    def load_tariff_files(file_paths):
        """
        Load all validated tariff files into PostgreSQL.
        """

        connection = get_postgres_connection()

        try:

            with connection.cursor() as cursor:

                for file_path in file_paths:

                    with open(
                        file_path,
                        "r",
                        encoding="utf-8",
                    ) as file:

                        reader = csv.DictReader(file)

                        for row in reader:

                            cursor.execute(
                                """
                                INSERT INTO tariff_reference (
                                    household_id,
                                    effective_date,
                                    tariff_rate,
                                    billing_tier,
                                    subsidy_flag
                                )
                                VALUES (
                                    %s,
                                    %s,
                                    %s,
                                    %s,
                                    %s
                                )

                                ON CONFLICT (
                                    household_id,
                                    effective_date
                                )

                                DO UPDATE SET
                                    tariff_rate =
                                        EXCLUDED.tariff_rate,

                                    billing_tier =
                                        EXCLUDED.billing_tier,

                                    subsidy_flag =
                                        EXCLUDED.subsidy_flag;
                                """,
                                (
                                    row["household_id"],
                                    row["effective_date"],

                                    float(
                                        row["tariff_rate"]
                                    ),

                                    row["billing_tier"],

                                    row[
                                        "subsidy_flag"
                                    ].lower()
                                    == "true",
                                ),
                            )

                    print(
                        f"Loaded tariff file: "
                        f"{Path(file_path).name}"
                    )

            connection.commit()

        finally:
            connection.close()

    tariff_files = find_tariff_files()

    valid_files = validate_tariff_files(
        tariff_files
    )

    load_tariff_files(
        valid_files
    )


tariff_ingestion()
