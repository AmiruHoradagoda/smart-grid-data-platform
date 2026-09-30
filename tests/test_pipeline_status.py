from datetime import date

import pytest

from utils.pipeline_status import (
    calculate_bills_for_date,
    find_ready_dates,
    is_complete_household_set,
    mark_billing_failed,
    update_tariff_readiness,
    write_daily_energy_rows,
)


class FakeCursor:
    def __init__(self, fetchone_results=None, fetchall_result=None, billed_rows=0):
        self.fetchone_results = list(fetchone_results or [])
        self.fetchall_result = list(fetchall_result or [])
        self.billed_rows = billed_rows
        self.rowcount = 0
        self.executions = []
        self.batches = []

    def execute(self, query, parameters=None):
        normalized_query = " ".join(query.split())
        self.executions.append((normalized_query, parameters))

        if "INSERT INTO daily_household_billing" in normalized_query:
            self.rowcount = self.billed_rows

    def executemany(self, query, parameters):
        self.batches.append((" ".join(query.split()), list(parameters)))

    def fetchone(self):
        return self.fetchone_results.pop(0)

    def fetchall(self):
        return self.fetchall_result


@pytest.mark.parametrize(
    "actual, expected, is_ready",
    [(20, 20, True), (19, 20, False), (21, 20, False)],
)
def test_complete_household_set_requires_exact_count(actual, expected, is_ready):
    assert is_complete_household_set(actual, expected) is is_ready


def test_daily_energy_write_is_idempotent_and_publishes_readiness():
    energy_date = date(2026, 1, 1)
    rows = [
        {
            "energy_date": energy_date,
            "household_id": "HH-001",
            "grid_zone": "ZONE-A",
            "total_consumption_kwh": 10.0,
            "total_solar_generation_kwh": 2.0,
            "total_grid_import_kwh": 8.0,
            "meter_readings": 288,
        },
        {
            "energy_date": energy_date,
            "household_id": "HH-002",
            "grid_zone": "ZONE-B",
            "total_consumption_kwh": 12.0,
            "total_solar_generation_kwh": 3.0,
            "total_grid_import_kwh": 9.0,
            "meter_readings": 288,
        },
    ]
    cursor = FakeCursor(fetchone_results=[(2,)])

    readiness = write_daily_energy_rows(cursor, rows, expected_households=2)

    assert readiness == {energy_date: (2, True)}
    assert len(cursor.batches) == 1
    assert len(cursor.batches[0][1]) == 2
    assert "ON CONFLICT (energy_date, household_id)" in cursor.batches[0][0]


def test_find_ready_dates_only_returns_database_selection():
    cursor = FakeCursor(
        fetchall_result=[
            (date(2026, 1, 1),),
            (date(2026, 1, 2),),
        ]
    )

    assert find_ready_dates(cursor) == [
        date(2026, 1, 1),
        date(2026, 1, 2),
    ]


def test_tariff_readiness_requires_all_households():
    cursor = FakeCursor(fetchone_results=[(19,)])

    households, is_ready = update_tariff_readiness(
        cursor,
        date(2026, 1, 1),
        expected_households=20,
    )

    assert households == 19
    assert is_ready is False
    query, parameters = cursor.executions[-1]
    assert "INSERT INTO daily_pipeline_status" in query
    assert parameters[1:3] == (False, 19)


def test_calculate_bills_marks_a_complete_date():
    cursor = FakeCursor(
        fetchone_results=[(20, 20)],
        billed_rows=20,
    )

    billed = calculate_bills_for_date(
        cursor,
        date(2026, 1, 1),
        expected_households=20,
    )

    assert billed == 20
    assert any(
        "billing_status = 'PROCESSING'" in query
        for query, _ in cursor.executions
    )
    assert any(
        "billing_status = 'COMPLETED'" in query
        for query, _ in cursor.executions
    )


def test_calculate_bills_rejects_incomplete_inputs():
    cursor = FakeCursor(fetchone_results=[(20, 19)])

    with pytest.raises(ValueError, match="expected 20 tariff rows"):
        calculate_bills_for_date(
            cursor,
            date(2026, 1, 1),
            expected_households=20,
        )


def test_failed_billing_error_is_bounded():
    cursor = FakeCursor()

    mark_billing_failed(
        cursor,
        date(2026, 1, 1),
        "x" * 1200,
    )

    _, parameters = cursor.executions[-1]
    assert len(parameters[0]) == 1000
