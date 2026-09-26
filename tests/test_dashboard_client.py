from datetime import date
from unittest.mock import Mock

import pytest
import requests

from dashboard import api_client


def test_missing_billing_is_empty(monkeypatch):
    response = Mock(status_code=404)
    get = Mock(return_value=response)
    monkeypatch.setattr(api_client.requests, "get", get)
    assert api_client.get_daily_billing(date(2099, 1, 1)) == []
    assert get.call_args.kwargs == {"params": {"billing_date": "2099-01-01"}, "timeout": 5}


def test_connection_failure_has_short_message(monkeypatch):
    monkeypatch.setattr(api_client.requests, "get", Mock(side_effect=requests.ConnectionError()))
    with pytest.raises(api_client.APIError, match="unavailable"):
        api_client.get_health()


def test_history_uses_limit_and_checks_status(monkeypatch):
    response = Mock(status_code=200)
    response.json.return_value = [{"grid_zone": "ZONE-A"}]
    get = Mock(return_value=response)
    monkeypatch.setattr(api_client.requests, "get", get)
    assert api_client.get_zone_history(300) == [{"grid_zone": "ZONE-A"}]
    assert get.call_args.kwargs["params"] == {"limit": 300}
    response.raise_for_status.assert_called_once()


def test_http_error_is_not_empty_data(monkeypatch):
    response = Mock(status_code=503)
    response.raise_for_status.side_effect = requests.HTTPError(response=response)
    monkeypatch.setattr(api_client.requests, "get", Mock(return_value=response))
    with pytest.raises(api_client.APIError, match="503"):
        api_client.get_daily_billing(date(2026, 1, 1))
