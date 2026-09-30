"""Durable coordination helpers for daily energy, tariffs, and billing."""

from datetime import date

BILLING_PENDING = "PENDING"
BILLING_PROCESSING = "PROCESSING"
BILLING_COMPLETED = "COMPLETED"
BILLING_FAILED = "FAILED"


def is_complete_household_set(actual_count, expected_count):
    """Return whether a simulated date contains exactly the expected households."""

    return actual_count == expected_count


def _row_value(row, column):
    """Read a named value from either a mapping or a PySpark Row."""

    try:
        return row[column]
    except (KeyError, TypeError):
        return getattr(row, column)


def update_energy_readiness(cursor, energy_date, expected_households):
    """Record whether Spark has stored a complete daily household result."""

    cursor.execute(
        """
        SELECT COUNT(*)
        FROM household_daily_energy
        WHERE energy_date = %s;
        """,
        (energy_date,),
    )

    household_count = int(cursor.fetchone()[0])
    is_ready = is_complete_household_set(
        household_count,
        expected_households,
    )

    cursor.execute(
        """
        INSERT INTO daily_pipeline_status (
            energy_date,
            energy_ready,
            energy_households,
            energy_ready_at,
            updated_at
        )
        VALUES (
            %s,
            %s,
            %s,
            CASE WHEN %s THEN CURRENT_TIMESTAMP ELSE NULL END,
            CURRENT_TIMESTAMP
        )
        ON CONFLICT (energy_date)
        DO UPDATE SET
            energy_ready = EXCLUDED.energy_ready,
            energy_households = EXCLUDED.energy_households,
            energy_ready_at = CASE
                WHEN EXCLUDED.energy_ready THEN
                    COALESCE(
                        daily_pipeline_status.energy_ready_at,
                        CURRENT_TIMESTAMP
                    )
                ELSE NULL
            END,
            updated_at = CURRENT_TIMESTAMP;
        """,
        (
            energy_date,
            is_ready,
            household_count,
            is_ready,
        ),
    )

    return household_count, is_ready


def update_tariff_readiness(cursor, effective_date, expected_households):
    """Record whether Airflow has stored a complete daily tariff result."""

    cursor.execute(
        """
        SELECT COUNT(*)
        FROM tariff_reference
        WHERE effective_date = %s;
        """,
        (effective_date,),
    )

    household_count = int(cursor.fetchone()[0])
    is_ready = is_complete_household_set(
        household_count,
        expected_households,
    )

    cursor.execute(
        """
        INSERT INTO daily_pipeline_status (
            energy_date,
            tariff_ready,
            tariff_households,
            tariff_ready_at,
            updated_at
        )
        VALUES (
            %s,
            %s,
            %s,
            CASE WHEN %s THEN CURRENT_TIMESTAMP ELSE NULL END,
            CURRENT_TIMESTAMP
        )
        ON CONFLICT (energy_date)
        DO UPDATE SET
            tariff_ready = EXCLUDED.tariff_ready,
            tariff_households = EXCLUDED.tariff_households,
            tariff_ready_at = CASE
                WHEN EXCLUDED.tariff_ready THEN
                    COALESCE(
                        daily_pipeline_status.tariff_ready_at,
                        CURRENT_TIMESTAMP
                    )
                ELSE NULL
            END,
            updated_at = CURRENT_TIMESTAMP;
        """,
        (
            effective_date,
            is_ready,
            household_count,
            is_ready,
        ),
    )

    return household_count, is_ready


def write_daily_energy_rows(cursor, rows, expected_households):
    """Upsert a small finalized Spark batch and publish its ready dates atomically."""

    records = [
        (
            _row_value(row, "energy_date"),
            _row_value(row, "household_id"),
            _row_value(row, "grid_zone"),
            _row_value(row, "total_consumption_kwh"),
            _row_value(row, "total_solar_generation_kwh"),
            _row_value(row, "total_grid_import_kwh"),
            _row_value(row, "meter_readings"),
        )
        for row in rows
    ]

    cursor.executemany(
        """
        INSERT INTO household_daily_energy (
            energy_date,
            household_id,
            grid_zone,
            total_consumption_kwh,
            total_solar_generation_kwh,
            total_grid_import_kwh,
            meter_readings
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (energy_date, household_id)
        DO UPDATE SET
            grid_zone = EXCLUDED.grid_zone,
            total_consumption_kwh = EXCLUDED.total_consumption_kwh,
            total_solar_generation_kwh =
                EXCLUDED.total_solar_generation_kwh,
            total_grid_import_kwh = EXCLUDED.total_grid_import_kwh,
            meter_readings = EXCLUDED.meter_readings;
        """,
        records,
    )

    readiness = {}
    energy_dates = sorted({record[0] for record in records})

    for energy_date in energy_dates:
        readiness[energy_date] = update_energy_readiness(
            cursor,
            energy_date,
            expected_households,
        )

    return readiness


def find_ready_dates(cursor):
    """Return dates whose energy and tariff inputs are ready for billing."""

    cursor.execute(
        """
        SELECT energy_date
        FROM daily_pipeline_status
        WHERE energy_ready = TRUE
          AND tariff_ready = TRUE
          AND billing_status IN ('PENDING', 'FAILED')
        ORDER BY energy_date;
        """
    )

    return [row[0] for row in cursor.fetchall()]


def calculate_bills_for_date(cursor, energy_date, expected_households):
    """Calculate one ready date and mark it complete in the same transaction."""

    if isinstance(energy_date, str):
        energy_date = date.fromisoformat(energy_date)

    cursor.execute(
        """
        SELECT
            (SELECT COUNT(*)
             FROM household_daily_energy
             WHERE energy_date = %s),
            (SELECT COUNT(*)
             FROM tariff_reference
             WHERE effective_date = %s);
        """,
        (energy_date, energy_date),
    )

    energy_count, tariff_count = (int(value) for value in cursor.fetchone())

    if not is_complete_household_set(energy_count, expected_households):
        raise ValueError(
            f"{energy_date}: expected {expected_households} energy rows, "
            f"found {energy_count}"
        )

    if not is_complete_household_set(tariff_count, expected_households):
        raise ValueError(
            f"{energy_date}: expected {expected_households} tariff rows, "
            f"found {tariff_count}"
        )

    cursor.execute(
        """
        UPDATE daily_pipeline_status
        SET billing_status = 'PROCESSING',
            billing_started_at = CURRENT_TIMESTAMP,
            last_error = NULL,
            updated_at = CURRENT_TIMESTAMP
        WHERE energy_date = %s;
        """,
        (energy_date,),
    )

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
            ON energy.household_id = tariff.household_id
           AND energy.energy_date = tariff.effective_date
        WHERE energy.energy_date = %s
        ON CONFLICT (energy_date, household_id)
        DO UPDATE SET
            grid_zone = EXCLUDED.grid_zone,
            total_consumption_kwh = EXCLUDED.total_consumption_kwh,
            total_solar_generation_kwh =
                EXCLUDED.total_solar_generation_kwh,
            total_grid_import_kwh = EXCLUDED.total_grid_import_kwh,
            tariff_rate = EXCLUDED.tariff_rate,
            billing_tier = EXCLUDED.billing_tier,
            subsidy_flag = EXCLUDED.subsidy_flag,
            estimated_bill_lkr = EXCLUDED.estimated_bill_lkr,
            calculated_at = CURRENT_TIMESTAMP;
        """,
        (energy_date,),
    )

    billed_households = cursor.rowcount

    if billed_households != expected_households:
        raise ValueError(
            f"{energy_date}: expected to bill {expected_households} households, "
            f"billed {billed_households}"
        )

    cursor.execute(
        """
        UPDATE daily_pipeline_status
        SET billing_status = 'COMPLETED',
            billed_at = CURRENT_TIMESTAMP,
            last_error = NULL,
            updated_at = CURRENT_TIMESTAMP
        WHERE energy_date = %s;
        """,
        (energy_date,),
    )

    return billed_households


def mark_billing_failed(cursor, energy_date, error):
    """Persist a retryable billing failure for operational visibility."""

    cursor.execute(
        """
        UPDATE daily_pipeline_status
        SET billing_status = 'FAILED',
            last_error = %s,
            updated_at = CURRENT_TIMESTAMP
        WHERE energy_date = %s;
        """,
        (str(error)[:1000], energy_date),
    )
