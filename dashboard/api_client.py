"""Read-only HTTP access to the existing FastAPI service."""

import os

import requests

API_BASE_URL = os.getenv("SMART_GRID_API_URL", "http://127.0.0.1:8000").rstrip("/")


class APIError(Exception):
    """A short, user-facing API failure message."""


def _get(path, params=None, missing_is_empty=False):
    try:
        response = requests.get(f"{API_BASE_URL}{path}", params=params, timeout=5)
        if response.status_code == 404 and missing_is_empty:
            return []
        response.raise_for_status()
        return response.json()
    except requests.HTTPError as error:
        raise APIError(f"API request failed (HTTP {error.response.status_code}).") from error
    except requests.RequestException as error:
        raise APIError("FastAPI is unavailable or the request timed out.") from error
    except ValueError as error:
        raise APIError("FastAPI returned an invalid response.") from error


def get_health():
    return _get("/health")


def get_latest_zones():
    return _get("/api/v1/zones/latest")


def get_zone_history(limit=100):
    return _get("/api/v1/zones/history", {"limit": limit})


def get_daily_billing(billing_date):
    return _get("/api/v1/billing/daily", {"billing_date": billing_date.isoformat()},
                missing_is_empty=True)


def get_renewable_alerts():
    return _get("/api/v1/alerts/renewable")
