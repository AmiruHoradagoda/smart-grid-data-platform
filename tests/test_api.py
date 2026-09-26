from datetime import date, datetime
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


@pytest.mark.parametrize("hour,pct,expected", [
    (10, 14.8, True), (10, 25, False), (10, 20, False),
    (5, 10, False), (6, 10, True), (17, 10, True), (18, 10, False),
])
def test_renewable_alerts(api, caplog, hour, pct, expected):
    client, connection, cursor, _ = api
    timestamp = datetime(2026, 1, 1, hour)
    cursor.fetchall.return_value = [{"grid_zone": "ZONE-B", "window_end": timestamp,
                                      "renewable_contribution_pct": pct}]
    with caplog.at_level("INFO", logger=main.__name__):
        response = client.get("/api/v1/alerts/renewable")
    assert response.status_code == 200
    assert response.json() == {"threshold_pct": 20, "alerts": [
        {"grid_zone": "ZONE-B", "window_end": timestamp.isoformat(),
         "renewable_contribution_pct": pct, "status": "LOW_RENEWABLE"}
    ] if expected else []}
    assert "zones_checked=1" in caplog.text
    assert ("low renewable contribution" in caplog.text) == expected
    connection.close.assert_called_once()


def test_alerts_empty_database(api):
    client, connection, _, _ = api
    assert client.get("/api/v1/alerts/renewable").json() == {"threshold_pct": 20, "alerts": []}
    connection.close.assert_called_once()


def test_alert_query_failure_cleanup(api):
    client, connection, cursor, _ = api
    cursor.execute.side_effect = psycopg2.OperationalError("query failed")
    with pytest.raises(psycopg2.OperationalError):
        client.get("/api/v1/alerts/renewable")
    connection.close.assert_called_once()


def test_alert_custom_configuration(api, monkeypatch):
    from utils.config_loader import Config
    monkeypatch.setitem(Config.load(), "alerts", {
        "low_renewable_threshold_pct": 30, "active_start_hour": 8, "active_end_hour": 16,
    })
    client, _, cursor, _ = api
    cursor.fetchall.return_value = [
        {"grid_zone": "ZONE-A", "window_end": datetime(2026, 1, 1, 7), "renewable_contribution_pct": 10},
        {"grid_zone": "ZONE-B", "window_end": datetime(2026, 1, 1, 10), "renewable_contribution_pct": 25},
    ]
    result = client.get("/api/v1/alerts/renewable").json()
    assert result["threshold_pct"] == 30
    assert [r["grid_zone"] for r in result["alerts"]] == ["ZONE-B"]
