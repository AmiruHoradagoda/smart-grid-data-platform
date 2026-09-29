from pathlib import Path

from tools.control_center_core import (
    CommandResult,
    DockerManager,
    LogBuffer,
    decode_kafka_event,
    host_service_specs,
    parse_engine_info,
    parse_stats,
    partition_sample_quotas,
)


class FakeRunner:
    def __init__(self):
        self.calls = []

    def run(self, args, **kwargs):
        self.calls.append((tuple(args), kwargs))
        return CommandResult(tuple(args), 0, "ok\n", "")


def test_parse_engine_info():
    assert parse_engine_info("29.7.2|6|4629180416\n") == {
        "version": "29.7.2",
        "cpus": 6,
        "memory_bytes": 4629180416,
    }


def test_parse_stats_ignores_malformed_rows():
    assert parse_stats(
        "smart-grid-postgres|30MiB / 4.3GiB|0.10%\ninvalid\n"
    ) == {
        "smart-grid-postgres": {
            "memory": "30MiB / 4.3GiB",
            "cpu": "0.10%",
        }
    }


def test_log_buffer_is_bounded():
    buffer = LogBuffer(max_lines=2)
    buffer.append("first")
    buffer.append("second")
    buffer.append("third")

    assert buffer.text() == "second\nthird"


def test_decode_kafka_event_exposes_partition_offset_and_meter_fields():
    event = decode_kafka_event(
        2,
        41,
        b"HH-007",
        (
            b'{"household_id":"HH-007","grid_zone":"ZONE-A",'
            b'"timestamp":"2026-01-01T12:00:00+00:00",'
            b'"power_consumption_kwh":0.42,"solar_generation_kwh":0.18}'
        ),
    )

    assert event.partition == 2
    assert event.offset == 41
    assert event.key == "HH-007"
    assert event.household_id == "HH-007"
    assert event.grid_zone == "ZONE-A"
    assert event.consumption_kwh == "0.42"
    assert event.solar_kwh == "0.18"


def test_partition_sample_quotas_are_bounded_and_even():
    assert partition_sample_quotas([0, 1, 2], 8) == {0: 3, 1: 3, 2: 2}
    assert partition_sample_quotas([0, 1, 2], 0) == {0: 0, 1: 0, 2: 0}


def test_host_commands_are_allowlisted_and_use_project_python():
    specs = host_service_specs()

    assert set(specs) == {"meter", "tariff", "api", "dashboard"}
    assert Path(specs["api"].command[0]).name == "python.exe"
    assert specs["api"].command[-2:] == ("--port", "8000")
    assert specs["dashboard"].port == 8501


def test_compose_down_preserves_volumes_by_default():
    runner = FakeRunner()
    manager = DockerManager(runner=runner)

    manager.compose_down()

    assert runner.calls[-1][0] == ("docker", "compose", "down")


def test_factory_reset_command_is_explicit():
    runner = FakeRunner()
    manager = DockerManager(runner=runner)

    manager.compose_down(remove_volumes=True)

    assert runner.calls[-1][0] == (
        "docker",
        "compose",
        "down",
        "-v",
        "--remove-orphans",
    )


def test_airflow_unpause_is_non_interactive():
    runner = FakeRunner()
    manager = DockerManager(runner=runner)

    manager.unpause_dags()

    assert runner.calls[-2][0] == (
        "docker",
        "exec",
        "smart-grid-airflow",
        "airflow",
        "dags",
        "unpause",
        "-y",
        "tariff_ingestion",
    )
    assert runner.calls[-1][0][-2:] == ("-y", "daily_billing")
