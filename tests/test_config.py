import pytest

from utils.config_loader import Config


@pytest.mark.parametrize("key, expected", [
    ("smart_meter.number_of_households", 20),
    ("kafka.topic", "smart-meter-readings"),
    ("postgres.database", "smart_grid"),
    ("alerts.low_renewable_threshold_pct", 20),
    ("simulation.simulated_day_seconds", 300),
])
def test_project_configuration(key, expected):
    assert Config.get(key) == expected


def test_config_path_does_not_depend_on_working_directory(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(Config, "_config", None)
    assert Config.get("postgres.database") == "smart_grid"
