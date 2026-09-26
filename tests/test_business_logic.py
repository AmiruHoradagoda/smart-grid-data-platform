import csv
from collections import Counter
from datetime import date

import pytest

from src.producers import smart_meter_producer as meter
from src.producers import tariff_generator as tariff


@pytest.mark.parametrize("hour", [0, 5, 18, 23])
def test_solar_is_zero_outside_daylight(hour):
    assert meter.calculate_solar_generation(hour, 0.5) == 0


def test_solar_peak_and_no_panel(monkeypatch):
    monkeypatch.setattr(meter.random, "uniform", lambda low, high: 1.0)
    assert meter.calculate_solar_generation(12, 0.5) == 0.5
    assert meter.calculate_solar_generation(12, 0) == 0


@pytest.mark.parametrize("hour, expected", [(0, 0.6), (6, 1.6), (12, 1.0), (18, 2.0)])
def test_consumption_daily_pattern(monkeypatch, hour, expected):
    monkeypatch.setattr(meter.random, "uniform", lambda low, high: 1.0)
    assert meter.calculate_consumption(hour, 1.0) == expected


@pytest.mark.parametrize("hour", [-1, 24])
def test_invalid_consumption_hour(hour):
    with pytest.raises(ValueError):
        meter.calculate_consumption(hour, 1.0)


def test_household_profiles():
    households = meter.build_households()
    assert len({h["household_id"] for h in households}) == 20
    assert sum(h["solar_capacity"] > 0 for h in households) == 13
    assert {h["grid_zone"] for h in households} == {"ZONE-A", "ZONE-B", "ZONE-C"}


def test_tariff_distribution_and_csv(tmp_path, monkeypatch):
    monkeypatch.setattr(tariff, "OUTPUT_DIRECTORY", tmp_path)
    profiles = tariff.build_tariff_profiles()
    assert Counter(p["billing_tier"] for p in profiles) == {"SUBSIDIZED": 4, "STANDARD": 12, "PREMIUM": 4}
    assert len({p["household_id"] for p in profiles}) == 20
    expected_rates = {"SUBSIDIZED": 32, "STANDARD": 42, "PREMIUM": 48}
    for profile in profiles:
        assert profile["tariff_rate"] == expected_rates[profile["billing_tier"]]
        assert profile["subsidy_flag"] == (profile["billing_tier"] == "SUBSIDIZED")
    path = tariff.generate_tariff_file(profiles, date(2026, 1, 1))
    with path.open(newline="", encoding="utf-8") as file:
        rows = list(csv.DictReader(file))
    assert len(rows) == 20
    assert {row["effective_date"] for row in rows} == {"2026-01-01"}


def test_tariff_household_count_mismatch(monkeypatch):
    monkeypatch.setattr(tariff, "NUMBER_OF_HOUSEHOLDS", 21)
    with pytest.raises(ValueError, match="household count"):
        tariff.build_tariff_profiles()


def test_tariff_reproducibility():
    assert tariff.RANDOM_SEED == 42
    assert tariff.build_tariff_profiles() == tariff.build_tariff_profiles()


def test_configured_consumption(monkeypatch):
    from utils.config_loader import Config
    monkeypatch.setitem(Config.get("smart_meter.consumption.multipliers"), "09_to_18", 1.5)
    monkeypatch.setattr(meter.random, "uniform", lambda low, high: 1.0)
    assert meter.calculate_consumption(12, 2) == 3


def test_configured_solar_boundaries(monkeypatch):
    from utils.config_loader import Config
    settings = Config.get("smart_meter.solar_generation")
    for key, value in [("sunrise_hour", 7), ("peak_hour", 11), ("sunset_hour", 17)]:
        monkeypatch.setitem(settings, key, value)
    monkeypatch.setattr(meter.random, "uniform", lambda low, high: 1.0)
    assert meter.calculate_solar_generation(6, 0.5) == 0
    assert meter.calculate_solar_generation(7, 0.5) == 0
    assert meter.calculate_solar_generation(11, 0.5) == 0.5
    assert meter.calculate_solar_generation(14, 0.5) > 0
    assert meter.calculate_solar_generation(17, 0.5) == 0
