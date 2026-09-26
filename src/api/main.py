from datetime import date
from contextlib import closing

import psycopg2
from psycopg2.extras import RealDictCursor
from fastapi import FastAPI, HTTPException

from utils.config_loader import Config


app = FastAPI(
    title="Smart Grid Data API",
    version="1.0.0",
)


def get_connection():
    return psycopg2.connect(
        host=Config.get("postgres.host.hostname"),
        port=Config.get("postgres.host.port"),
        dbname=Config.get("postgres.database"),
        user=Config.get("postgres.user"),
        password=Config.get("postgres.password"),
        connect_timeout=5,
        cursor_factory=RealDictCursor,
    )


@app.get("/health")
def health_check():
    try:
        with closing(get_connection()) as connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT 1;")
                cursor.fetchone()

        return {
            "status": "healthy",
            "database": "connected",
        }

    except Exception as error:
        raise HTTPException(
            status_code=503,
            detail=f"Database unavailable: {error}",
        )


@app.get("/api/v1/zones/latest")
def get_latest_zone_metrics():
    connection = get_connection()

    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT DISTINCT ON (grid_zone)
                    grid_zone,
                    window_start,
                    window_end,
                    total_consumption_kwh,
                    total_solar_generation_kwh,
                    total_grid_import_kwh,
                    renewable_contribution_pct,
                    meter_readings
                FROM zone_energy_metrics
                ORDER BY
                    grid_zone,
                    window_end DESC;
                """
            )

            return cursor.fetchall()

    finally:
        connection.close()


@app.get("/api/v1/billing/daily")
def get_daily_billing(billing_date: date):
    connection = get_connection()

    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
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
                FROM daily_household_billing
                WHERE energy_date = %s
                ORDER BY household_id;
                """,
                (billing_date,),
            )

            rows = cursor.fetchall()

            if not rows:
                raise HTTPException(
                    status_code=404,
                    detail="No billing data found for this date",
                )

            return rows

    finally:
        connection.close()


@app.get("/api/v1/households/{household_id}/billing")
def get_household_billing(household_id: str):
    connection = get_connection()

    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
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
                FROM daily_household_billing
                WHERE household_id = %s
                ORDER BY energy_date;
                """,
                (household_id,),
            )

            rows = cursor.fetchall()

            if not rows:
                raise HTTPException(
                    status_code=404,
                    detail=f"No billing data found for {household_id}",
                )

            return rows

    finally:
        connection.close()
