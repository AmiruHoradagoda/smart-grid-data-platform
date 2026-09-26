from datetime import date
from decimal import Decimal
from unittest.mock import MagicMock

import psycopg2
import pytest
from fastapi.testclient import TestClient

from src.api import main


@pytest.fixture
def api(monkeypatch):
    connection = MagicMock()
    cursor = connection.cursor.return_value.__enter__.return_value
    cursor.fetchall.return_value = []
    connect = MagicMock(return_value=connection)
    monkeypatch.setattr(main, "get_connection", connect)
    with TestClient(main.app) as client:
        yield client, connection, cursor, connect


def test_health_success(api):
    client, connection, cursor, _ = api
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "healthy", "database": "connected"}
    cursor.execute.assert_called_once_with("SELECT 1;")
    connection.close.assert_called_once()


def test_health_connection_failure(api):
    client, _, _, connect = api
    connect.side_effect = psycopg2.OperationalError("unavailable")
    assert client.get("/health").status_code == 503


def test_health_query_failure_closes_connection(api):
    client, connection, cursor, _ = api
    cursor.execute.side_effect = psycopg2.OperationalError("query failed")
    assert client.get("/health").status_code == 503
    connection.close.assert_called_once()


@pytest.mark.parametrize("query", ["", "?billing_date=invalid", "?billing_date=2026-02-30"])
def test_invalid_billing_date_does_not_connect(api, query):
    client, _, _, connect = api
    assert client.get("/api/v1/billing/daily" + query).status_code == 422
    connect.assert_not_called()


def test_missing_billing_date(api):
    client, connection, cursor, _ = api
    response = client.get("/api/v1/billing/daily?billing_date=2099-01-01")
    assert response.status_code == 404
    assert cursor.execute.call_args.args[1] == (date(2099, 1, 1),)
    connection.close.assert_called_once()


@pytest.mark.parametrize("household", ["HH-999", "not-a-household"])
def test_unknown_household(api, household):
    client, connection, cursor, _ = api
    assert client.get(f"/api/v1/households/{household}/billing").status_code == 404
    assert cursor.execute.call_args.args[1] == (household,)
    connection.close.assert_called_once()


def test_billing_response_serialization(api):
    client, connection, cursor, _ = api
    cursor.fetchall.return_value = [{"household_id": "HH-001", "energy_date": date(2026, 1, 1), "estimated_bill_lkr": Decimal("420.00")}]
    response = client.get("/api/v1/billing/daily?billing_date=2026-01-01")
    assert response.status_code == 200
    assert response.json() == [{"household_id": "HH-001", "energy_date": "2026-01-01", "estimated_bill_lkr": 420.0}]
    connection.close.assert_called_once()


def test_latest_zones(api):
    client, connection, cursor, _ = api
    cursor.fetchall.return_value = [{"grid_zone": "ZONE-A", "renewable_contribution_pct": 25.0}]
    response = client.get("/api/v1/zones/latest")
    assert response.status_code == 200
    assert response.json()[0]["grid_zone"] == "ZONE-A"
    connection.close.assert_called_once()


@pytest.mark.skip(reason="Renewable alert endpoint is not implemented; only config exists.")
def test_renewable_alert_response_structure():
    """Add a response contract test when the endpoint has an actual schema."""
