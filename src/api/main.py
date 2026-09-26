from datetime import date
from contextlib import closing
import logging

import psycopg2
from psycopg2.extras import RealDictCursor
from fastapi import FastAPI, HTTPException, Query

from utils.config_loader import Config

logger = logging.getLogger(__name__)


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


@app.get("/api/v1/zones/history")
def get_zone_history(limit: int = Query(default=100, ge=1, le=1000)):
    with closing(get_connection()) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT * FROM (
                    SELECT window_start, window_end, grid_zone,
                           total_consumption_kwh, total_solar_generation_kwh,
                           total_grid_import_kwh, renewable_contribution_pct,
                           meter_readings
                    FROM zone_energy_metrics
                    ORDER BY window_end DESC, grid_zone, window_start DESC
                    LIMIT %s
                ) AS recent
                ORDER BY window_end, grid_zone, window_start;
                """,
                (limit,),
            )
            return cursor.fetchall()


@app.get("/api/v1/alerts/renewable")
def get_renewable_alerts():
    threshold = Config.get("alerts.low_renewable_threshold_pct")
    start_hour = Config.get("alerts.active_start_hour")
    end_hour = Config.get("alerts.active_end_hour")
    # Reuse the latest-per-zone query; it closes its connection before returning.
    zones = get_latest_zone_metrics()
    alerts = []
    for zone in zones:
        if (start_hour <= zone["window_end"].hour < end_hour
                and zone["renewable_contribution_pct"] < threshold):
            alerts.append({
                "grid_zone": zone["grid_zone"],
                "renewable_contribution_pct": zone["renewable_contribution_pct"],
                "status": "LOW_RENEWABLE",
                "window_end": zone["window_end"],
            })
            logger.warning(
                "low renewable contribution grid_zone=%s renewable_pct=%s threshold_pct=%s",
                zone["grid_zone"], zone["renewable_contribution_pct"], threshold,
            )
    logger.info("renewable alert check completed zones_checked=%s alerts_found=%s",
                len(zones), len(alerts))
    return {"threshold_pct": threshold, "alerts": alerts}


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
