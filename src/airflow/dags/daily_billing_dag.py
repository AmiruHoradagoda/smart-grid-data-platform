from datetime import datetime

import psycopg2

from airflow.sdk import dag, task

from utils.config_loader import Config


def get_postgres_connection():
    return psycopg2.connect(
        host=Config.get("postgres.docker.hostname"),
        port=Config.get("postgres.docker.port"),
        dbname=Config.get("postgres.database"),
        user=Config.get("postgres.user"),
        password=Config.get("postgres.password"),
    )


@dag(
    schedule="*/5 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["smart-grid", "billing"],
)
def daily_billing():

    @task
    def calculate_daily_bills():

        connection = get_postgres_connection()

        try:
            with connection.cursor() as cursor:

                cursor.execute(
                    """
                    INSERT INTO daily_household_billing (
                        energy_date,
                        household_id,
                        grid_zone,

                        total_consumption_kwh,
                        total_solar_generation_kwh,
                        total_grid_import_kwh,

                        tariff_rate,
                        billing_tier,
                        subsidy_flag,

                        estimated_bill_lkr
                    )

                    SELECT
                        energy.energy_date,
                        energy.household_id,
                        energy.grid_zone,

                        energy.total_consumption_kwh,
                        energy.total_solar_generation_kwh,
                        energy.total_grid_import_kwh,

                        tariff.tariff_rate,
                        tariff.billing_tier,
                        tariff.subsidy_flag,

                        ROUND(
                            (
                                energy.total_grid_import_kwh
                                * tariff.tariff_rate
                            )::numeric,
                            2
                        )

                    FROM household_daily_energy AS energy

                    INNER JOIN tariff_reference AS tariff
                        ON energy.household_id =
                           tariff.household_id

                        AND energy.energy_date =
                            tariff.effective_date

                    ON CONFLICT (
                        energy_date,
                        household_id
                    )

                    DO UPDATE SET
                        grid_zone =
                            EXCLUDED.grid_zone,

                        total_consumption_kwh =
                            EXCLUDED.total_consumption_kwh,

                        total_solar_generation_kwh =
                            EXCLUDED.total_solar_generation_kwh,

                        total_grid_import_kwh =
                            EXCLUDED.total_grid_import_kwh,

                        tariff_rate =
                            EXCLUDED.tariff_rate,

                        billing_tier =
                            EXCLUDED.billing_tier,

                        subsidy_flag =
                            EXCLUDED.subsidy_flag,

                        estimated_bill_lkr =
                            EXCLUDED.estimated_bill_lkr,

                        calculated_at =
                            CURRENT_TIMESTAMP;
                    """
                )

            connection.commit()

        finally:
            connection.close()

    calculate_daily_bills()


daily_billing()
