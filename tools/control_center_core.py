"""Process, Docker, and status helpers for the optional desktop control center."""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from pathlib import Path
import os
import signal
import socket
import subprocess
import sys
import threading
import time
from typing import Callable, Iterable


PROJECT_ROOT = Path(__file__).resolve().parents[1]

CONTAINERS = {
    "postgres": "smart-grid-postgres",
    "kafka": "smart-grid-kafka",
    "spark": "smart-grid-spark",
    "airflow": "smart-grid-airflow",
}


@dataclass(frozen=True)
class CommandResult:
    """Captured result of one non-interactive command."""

    args: tuple[str, ...]
    returncode: int
    stdout: str
    stderr: str

    @property
    def ok(self) -> bool:
        return self.returncode == 0

    @property
    def output(self) -> str:
        return "\n".join(
            value.strip()
            for value in (self.stdout, self.stderr)
            if value.strip()
        )


class CommandError(RuntimeError):
    """Raised when an allowed project command fails."""


class CommandRunner:
    """Run argument-list commands without invoking a shell."""

    def __init__(self, workdir: Path = PROJECT_ROOT):
        self.workdir = Path(workdir)

    def run(
        self,
        args: Iterable[str],
        *,
        timeout: int = 120,
        input_text: str | None = None,
    ) -> CommandResult:
        command = tuple(str(value) for value in args)
        creationflags = 0
        if os.name == "nt":
            creationflags = getattr(subprocess, "CREATE_NO_WINDOW", 0)

        try:
            completed = subprocess.run(
                command,
                cwd=self.workdir,
                input=input_text,
                text=True,
                encoding="utf-8",
                errors="replace",
                capture_output=True,
                timeout=timeout,
                check=False,
                creationflags=creationflags,
            )
        except FileNotFoundError as error:
            raise CommandError(f"Command not found: {command[0]}") from error
        except subprocess.TimeoutExpired as error:
            raise CommandError(
                f"Command timed out after {timeout}s: {' '.join(command)}"
            ) from error

        return CommandResult(
            args=command,
            returncode=completed.returncode,
            stdout=completed.stdout or "",
            stderr=completed.stderr or "",
        )


def require_success(result: CommandResult, label: str) -> CommandResult:
    """Return a successful result or raise a concise operational error."""

    if result.ok:
        return result

    detail = result.output or f"exit code {result.returncode}"
    raise CommandError(f"{label} failed: {detail}")


class LogBuffer:
    """Thread-safe, bounded in-memory process log."""

    def __init__(self, max_lines: int = 750):
        self._lines: deque[str] = deque(maxlen=max_lines)
        self._lock = threading.Lock()

    def append(self, line: str) -> None:
        with self._lock:
            self._lines.append(line.rstrip("\r\n"))

    def text(self) -> str:
        with self._lock:
            return "\n".join(self._lines)


@dataclass(frozen=True)
class HostServiceSpec:
    """An allowlisted host process controlled by the application."""

    key: str
    label: str
    command: tuple[str, ...]
    port: int | None = None


def project_python() -> Path:
    """Prefer the project virtual environment, falling back to this interpreter."""

    if os.name == "nt":
        candidate = PROJECT_ROOT / ".venv" / "Scripts" / "python.exe"
    else:
        candidate = PROJECT_ROOT / ".venv" / "bin" / "python"

    return candidate if candidate.exists() else Path(sys.executable)


def host_service_specs() -> dict[str, HostServiceSpec]:
    """Return the fixed host-process allowlist."""

    python = str(project_python())
    return {
        "meter": HostServiceSpec(
            key="meter",
            label="Meter producer",
            command=(python, "-m", "src.producers.smart_meter_producer"),
        ),
        "tariff": HostServiceSpec(
            key="tariff",
            label="Tariff generator",
            command=(python, "-m", "src.producers.tariff_generator"),
        ),
        "api": HostServiceSpec(
            key="api",
            label="FastAPI",
            command=(
                python,
                "-m",
                "uvicorn",
                "src.api.main:app",
                "--host",
                "127.0.0.1",
                "--port",
                "8000",
            ),
            port=8000,
        ),
        "dashboard": HostServiceSpec(
            key="dashboard",
            label="Streamlit dashboard",
            command=(
                python,
                "-m",
                "streamlit",
                "run",
                "dashboard/app.py",
                "--server.address",
                "127.0.0.1",
                "--server.port",
                "8501",
                "--server.headless",
                "true",
            ),
            port=8501,
        ),
    }


@dataclass
class ManagedHostProcess:
    spec: HostServiceSpec
    process: subprocess.Popen[str]
    log: LogBuffer


class HostProcessManager:
    """Own and safely stop only host processes launched by this instance."""

    def __init__(self, workdir: Path = PROJECT_ROOT):
        self.workdir = Path(workdir)
        self.specs = host_service_specs()
        self._processes: dict[str, ManagedHostProcess] = {}
        self._lock = threading.Lock()

    def start(
        self,
        key: str,
        on_line: Callable[[str, str], None] | None = None,
    ) -> ManagedHostProcess:
        spec = self.specs[key]

        with self._lock:
            current = self._processes.get(key)
            if current and current.process.poll() is None:
                raise CommandError(f"{spec.label} is already running.")

            if spec.port and port_is_open(spec.port):
                raise CommandError(
                    f"Port {spec.port} is already in use. "
                    f"{spec.label} may be running externally."
                )

            creationflags = 0
            if os.name == "nt":
                creationflags = (
                    getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
                    | getattr(subprocess, "CREATE_NO_WINDOW", 0)
                )

            environment = os.environ.copy()
            environment["PYTHONUNBUFFERED"] = "1"
            environment.setdefault("SMART_GRID_API_URL", "http://127.0.0.1:8000")

            process = subprocess.Popen(
                spec.command,
                cwd=self.workdir,
                env=environment,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                encoding="utf-8",
                errors="replace",
                bufsize=1,
                creationflags=creationflags,
            )
            managed = ManagedHostProcess(spec=spec, process=process, log=LogBuffer())
            self._processes[key] = managed

        thread = threading.Thread(
            target=self._capture_output,
            args=(managed, on_line),
            daemon=True,
        )
        thread.start()
        return managed

    @staticmethod
    def _capture_output(
        managed: ManagedHostProcess,
        on_line: Callable[[str, str], None] | None,
    ) -> None:
        if managed.process.stdout is None:
            return

        for line in managed.process.stdout:
            managed.log.append(line)
            if on_line:
                on_line(managed.spec.key, line.rstrip("\r\n"))

    def stop(self, key: str, graceful_seconds: int = 5) -> bool:
        with self._lock:
            managed = self._processes.get(key)

        if not managed or managed.process.poll() is not None:
            return False

        process = managed.process
        if os.name == "nt":
            try:
                process.send_signal(signal.CTRL_BREAK_EVENT)
            except (OSError, ValueError):
                process.terminate()
        else:
            process.send_signal(signal.SIGINT)

        try:
            process.wait(timeout=graceful_seconds)
        except subprocess.TimeoutExpired:
            process.terminate()
            try:
                process.wait(timeout=3)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=3)

        return True

    def stop_all(self) -> None:
        for key in tuple(self.specs):
            self.stop(key)

    def status(self, key: str) -> str:
        with self._lock:
            managed = self._processes.get(key)

        if managed and managed.process.poll() is None:
            return f"Running (PID {managed.process.pid})"

        spec = self.specs[key]
        if spec.port and port_is_open(spec.port):
            return "Running externally"

        if managed and managed.process.returncode not in (None, 0):
            return f"Exited ({managed.process.returncode})"

        return "Stopped"

    def log_text(self, key: str) -> str:
        with self._lock:
            managed = self._processes.get(key)
        return managed.log.text() if managed else "No captured output."


def port_is_open(port: int, host: str = "127.0.0.1") -> bool:
    """Return whether a local TCP port accepts a connection."""

    try:
        with socket.create_connection((host, port), timeout=0.25):
            return True
    except OSError:
        return False


def parse_engine_info(output: str) -> dict[str, int | str]:
    """Parse the control center's stable Docker info format."""

    version, cpus, memory = output.strip().split("|", maxsplit=2)
    return {
        "version": version,
        "cpus": int(cpus),
        "memory_bytes": int(memory),
    }


def parse_stats(output: str) -> dict[str, dict[str, str]]:
    """Parse one-line-per-container Docker stats output."""

    parsed: dict[str, dict[str, str]] = {}
    for line in output.splitlines():
        fields = line.split("|", maxsplit=2)
        if len(fields) == 3:
            parsed[fields[0]] = {"memory": fields[1], "cpu": fields[2]}
    return parsed


class DockerManager:
    """Allowlisted Docker Compose operations for this repository."""

    def __init__(self, runner: CommandRunner | None = None):
        self.runner = runner or CommandRunner()

    def engine_info(self) -> dict[str, int | str]:
        result = require_success(
            self.runner.run(
                [
                    "docker",
                    "info",
                    "--format",
                    "{{.ServerVersion}}|{{.NCPU}}|{{.MemTotal}}",
                ],
                timeout=15,
            ),
            "Docker engine check",
        )
        return parse_engine_info(result.stdout)

    def compose_up(self, *services: str) -> CommandResult:
        args = ["docker", "compose", "up", "-d", *services]
        return require_success(self.runner.run(args, timeout=900), "Compose start")

    def compose_stop(self, *services: str) -> CommandResult:
        args = ["docker", "compose", "stop", *services]
        return require_success(self.runner.run(args, timeout=180), "Compose stop")

    def compose_down(self, *, remove_volumes: bool = False) -> CommandResult:
        args = ["docker", "compose", "down"]
        if remove_volumes:
            args.extend(["-v", "--remove-orphans"])
        return require_success(self.runner.run(args, timeout=240), "Compose down")

    def build_spark(self) -> CommandResult:
        return require_success(
            self.runner.run(["docker", "compose", "build", "spark"], timeout=1200),
            "Spark image build",
        )

    def container_states(self) -> dict[str, dict[str, str]]:
        states: dict[str, dict[str, str]] = {}
        template = "{{.State.Status}}|{{if .State.Health}}{{.State.Health.Status}}{{end}}"

        for service, container in CONTAINERS.items():
            result = self.runner.run(
                ["docker", "inspect", "--format", template, container],
                timeout=10,
            )
            if not result.ok:
                states[service] = {"status": "Missing", "health": ""}
                continue

            values = result.stdout.strip().split("|", maxsplit=1)
            states[service] = {
                "status": values[0].capitalize(),
                "health": values[1].capitalize() if len(values) > 1 else "",
            }

        return states

    def resource_stats(self) -> dict[str, dict[str, str]]:
        result = self.runner.run(
            [
                "docker",
                "stats",
                "--no-stream",
                "--format",
                "{{.Name}}|{{.MemUsage}}|{{.CPUPerc}}",
            ],
            timeout=20,
        )
        return parse_stats(result.stdout) if result.ok else {}

    def wait_for_health(self, services: Iterable[str], timeout: int = 150) -> None:
        deadline = time.monotonic() + timeout
        pending = set(services)

        while pending and time.monotonic() < deadline:
            states = self.container_states()
            pending = {
                service
                for service in pending
                if states[service]["health"].lower() != "healthy"
            }
            if pending:
                time.sleep(2)

        if pending:
            raise CommandError(
                "Timed out waiting for healthy services: " + ", ".join(sorted(pending))
            )

    def bootstrap(self) -> str:
        messages: list[str] = []

        database_check = require_success(
            self.runner.run(
                [
                    "docker",
                    "exec",
                    CONTAINERS["postgres"],
                    "psql",
                    "-U",
                    "smartgrid",
                    "-d",
                    "postgres",
                    "-tAc",
                    "SELECT 1 FROM pg_database WHERE datname='airflow_meta';",
                ]
            ),
            "Airflow database check",
        )
        if database_check.stdout.strip() != "1":
            require_success(
                self.runner.run(
                    [
                        "docker",
                        "exec",
                        CONTAINERS["postgres"],
                        "createdb",
                        "-U",
                        "smartgrid",
                        "airflow_meta",
                    ]
                ),
                "Airflow database creation",
            )
            messages.append("Created airflow_meta database.")
        else:
            messages.append("airflow_meta database already exists.")

        migration = PROJECT_ROOT / "database" / "migrations" / "001_daily_pipeline_status.sql"
        if migration.exists():
            require_success(
                self.runner.run(
                    [
                        "docker",
                        "exec",
                        "-i",
                        CONTAINERS["postgres"],
                        "psql",
                        "-v",
                        "ON_ERROR_STOP=1",
                        "-U",
                        "smartgrid",
                        "-d",
                        "smart_grid",
                    ],
                    input_text=migration.read_text(encoding="utf-8"),
                ),
                "Database migration",
            )
            messages.append("Applied database migration.")

        topic = require_success(
            self.runner.run(
                [
                    "docker",
                    "exec",
                    CONTAINERS["kafka"],
                    "/opt/kafka/bin/kafka-topics.sh",
                    "--bootstrap-server",
                    "kafka:29092",
                    "--create",
                    "--if-not-exists",
                    "--topic",
                    "smart-meter-readings",
                    "--partitions",
                    "3",
                    "--replication-factor",
                    "1",
                ],
                timeout=60,
            ),
            "Kafka topic creation",
        )
        messages.append(topic.output or "Kafka topic is ready.")
        return "\n".join(messages)

    def wait_for_airflow(self, timeout: int = 240) -> None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            result = self.runner.run(
                ["docker", "exec", CONTAINERS["airflow"], "airflow", "dags", "list"],
                timeout=30,
            )
            if result.ok:
                return
            time.sleep(5)
        raise CommandError("Timed out waiting for Airflow DAG discovery.")

    def unpause_dags(self) -> str:
        outputs = []
        for dag_id in ("tariff_ingestion", "daily_billing"):
            result = require_success(
                self.runner.run(
                    [
                        "docker",
                        "exec",
                        CONTAINERS["airflow"],
                        "airflow",
                        "dags",
                        "unpause",
                        dag_id,
                    ]
                ),
                f"Unpause {dag_id}",
            )
            outputs.append(result.output)
        return "\n".join(value for value in outputs if value)

    def trigger_dag(self, dag_id: str) -> CommandResult:
        if dag_id not in {"tariff_ingestion", "daily_billing"}:
            raise CommandError(f"Unsupported DAG: {dag_id}")
        return require_success(
            self.runner.run(
                [
                    "docker",
                    "exec",
                    CONTAINERS["airflow"],
                    "airflow",
                    "dags",
                    "trigger",
                    dag_id,
                ]
            ),
            f"Trigger {dag_id}",
        )

    def logs(self, service: str, tail: int = 100) -> str:
        if service not in CONTAINERS:
            raise CommandError(f"Unsupported service: {service}")
        result = self.runner.run(
            ["docker", "compose", "logs", f"--tail={tail}", service],
            timeout=30,
        )
        return result.output or "No logs available."


def fetch_pipeline_status() -> list[dict[str, object]]:
    """Read the durable daily state directly from host PostgreSQL."""

    try:
        import psycopg2
        from psycopg2.extras import RealDictCursor
    except ImportError:
        return []

    try:
        connection = psycopg2.connect(
            host="127.0.0.1",
            port=5433,
            dbname="smart_grid",
            user="smartgrid",
            password="smartgrid",
            connect_timeout=2,
            cursor_factory=RealDictCursor,
        )
    except psycopg2.Error:
        return []

    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT energy_date, energy_ready, tariff_ready,
                       energy_households, tariff_households,
                       billing_status, COALESCE(last_error, '') AS last_error
                FROM daily_pipeline_status
                ORDER BY energy_date DESC
                LIMIT 30;
                """
            )
            return [dict(row) for row in cursor.fetchall()]
    except psycopg2.Error:
        return []
    finally:
        connection.close()
