import logging

from datetime import date, datetime, timedelta

import psycopg2

from airflow.sdk import dag, task

from utils.config_loader import Config
from utils.pipeline_status import (
    calculate_bills_for_date,
    find_ready_dates as select_ready_dates,
    mark_billing_failed,
)


logger = logging.getLogger(__name__)


def get_postgres_connection():
    return psycopg2.connect(
        host=Config.get("postgres.docker.hostname"),
        port=Config.get("postgres.docker.port"),
        dbname=Config.get("postgres.database"),
        user=Config.get("postgres.user"),
        password=Config.get("postgres.password"),
    )


@dag(
    schedule=Config.get("orchestration.billing_dispatch_schedule"),
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["smart-grid", "billing"],
)
def daily_billing():

    @task(retries=1, retry_delay=timedelta(seconds=30))
    def find_ready_dates():
        """Find durable simulated dates whose two inputs are complete."""

        connection = get_postgres_connection()

        try:
            with connection.cursor() as cursor:
                ready_dates = select_ready_dates(cursor)

            logger.info(
                "billing_dates_discovered count=%s dates=%s",
                len(ready_dates),
                ",".join(str(value) for value in ready_dates),
            )

            # ISO strings are explicit and serialize cleanly through XCom.
            return [value.isoformat() for value in ready_dates]

        finally:
            connection.close()

    @task(retries=1, retry_delay=timedelta(seconds=30))
    def calculate_daily_bills(ready_dates):
        """Bill every ready date, preserving failures for a later retry."""

        if not ready_dates:
            logger.info("billing_dispatch_no_ready_dates")
            return 0

        connection = get_postgres_connection()
        expected_households = Config.get(
            "smart_meter.number_of_households"
        )
        failures = []
        total_billed = 0

        try:
            for value in ready_dates:
                energy_date = date.fromisoformat(value)

                try:
                    with connection.cursor() as cursor:
                        billed_households = calculate_bills_for_date(
                            cursor,
                            energy_date,
                            expected_households,
                        )

                    connection.commit()
                    total_billed += billed_households

                    logger.info(
                        "billing_completed "
                        "energy_date=%s households=%s",
                        energy_date,
                        billed_households,
                    )

                except Exception as error:
                    connection.rollback()

                    with connection.cursor() as cursor:
                        mark_billing_failed(
                            cursor,
                            energy_date,
                            error,
                        )

                    connection.commit()
                    failures.append(f"{energy_date}: {error}")

                    logger.exception(
                        "billing_failed energy_date=%s",
                        energy_date,
                    )

        finally:
            connection.close()

        if failures:
            raise RuntimeError(
                "Billing failed for ready dates: "
                + "; ".join(failures)
            )

        return total_billed

    ready_dates = find_ready_dates()
    calculate_daily_bills(ready_dates)


daily_billing()
