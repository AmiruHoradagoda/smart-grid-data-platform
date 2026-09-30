"""Optional Tkinter control center for the local smart-grid platform."""

from __future__ import annotations

from pathlib import Path
from queue import Empty, Queue
import sys
import threading
import tkinter as tk
from tkinter import messagebox, scrolledtext, simpledialog, ttk
import webbrowser


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from tools.control_center_core import (  # noqa: E402
    CONTAINERS,
    CommandError,
    DockerManager,
    HostProcessManager,
    fetch_pipeline_status,
)


REFRESH_MS = 5000


class ControlCenter:
    """Small Windows-first UI for safe, allowlisted project operations."""

    def __init__(self, root: tk.Tk):
        self.root = root
        self.root.title("Smart Grid Control Center")
        self.root.geometry("1180x820")
        self.root.minsize(980, 700)

        self.docker = DockerManager()
        self.host = HostProcessManager()
        self.ui_queue: Queue[tuple[object, tuple[object, ...]]] = Queue()
        self.action_lock = threading.Lock()
        self.refresh_lock = threading.Lock()
        self.refresh_after_id: str | None = None

        self.status_var = tk.StringVar(value="Ready. The control center is optional.")
        self.engine_var = tk.StringVar(value="Docker: checking...")
        self.memory_var = tk.StringVar(value="Memory: checking...")
        self.log_source_var = tk.StringVar(value="spark")
        self.container_service_var = tk.StringVar(value="spark")
        self.dag_var = tk.StringVar(value="daily_billing")

        self._configure_style()
        self._build_ui()
        self.root.protocol("WM_DELETE_WINDOW", self._on_close)
        self.root.after(100, self._drain_ui_queue)
        self.root.after(250, self.refresh_async)

    def _configure_style(self) -> None:
        style = ttk.Style(self.root)
        if "vista" in style.theme_names():
            style.theme_use("vista")
        style.configure("Title.TLabel", font=("Segoe UI", 16, "bold"))
        style.configure("Section.TLabelframe.Label", font=("Segoe UI", 10, "bold"))
        style.configure("Danger.TButton", foreground="#a00000")

    def _build_ui(self) -> None:
        outer = ttk.Frame(self.root, padding=10)
        outer.pack(fill="both", expand=True)

        header = ttk.Frame(outer)
        header.pack(fill="x")
        ttk.Label(header, text="Smart Grid Control Center", style="Title.TLabel").pack(
            side="left"
        )
        ttk.Button(header, text="Refresh", command=self.refresh_async).pack(side="right")
        ttk.Label(header, textvariable=self.memory_var).pack(side="right", padx=14)
        ttk.Label(header, textvariable=self.engine_var).pack(side="right", padx=14)

        quick = ttk.LabelFrame(
            outer,
            text="Guided operations",
            padding=8,
            style="Section.TLabelframe",
        )
        quick.pack(fill="x", pady=(10, 6))
        ttk.Button(quick, text="Start Infrastructure", command=self.start_infrastructure).pack(
            side="left", padx=3
        )
        ttk.Button(quick, text="Bootstrap DB + Kafka", command=self.bootstrap).pack(
            side="left", padx=3
        )
        ttk.Button(quick, text="Start Spark + Airflow", command=self.start_compute).pack(
            side="left", padx=3
        )
        ttk.Button(quick, text="Guided Full Start", command=self.guided_full_start).pack(
            side="left", padx=3
        )
        ttk.Button(quick, text="Stop All (Keep Data)", command=self.stop_all).pack(
            side="right", padx=3
        )
        ttk.Button(
            quick,
            text="Factory Reset",
            style="Danger.TButton",
            command=self.factory_reset,
        ).pack(side="right", padx=3)

        notebook = ttk.Notebook(outer)
        notebook.pack(fill="both", expand=True, pady=5)

        overview = ttk.Frame(notebook, padding=8)
        logs_tab = ttk.Frame(notebook, padding=8)
        notebook.add(overview, text="Overview")
        notebook.add(logs_tab, text="Recent logs")

        self._build_overview(overview)
        self._build_logs(logs_tab)

        status = ttk.Frame(outer)
        status.pack(fill="x", pady=(6, 0))
        ttk.Separator(status).pack(fill="x", pady=(0, 5))
        ttk.Label(status, textvariable=self.status_var).pack(side="left")
        ttk.Label(
            status,
            text="Closing this window does not stop Docker containers.",
            foreground="#555555",
        ).pack(side="right")

    def _build_overview(self, parent: ttk.Frame) -> None:
        upper = ttk.Panedwindow(parent, orient="horizontal")
        upper.pack(fill="both", expand=True)

        containers_frame = ttk.LabelFrame(
            upper, text="Docker services", padding=6, style="Section.TLabelframe"
        )
        host_frame = ttk.LabelFrame(
            upper, text="Host applications", padding=6, style="Section.TLabelframe"
        )
        upper.add(containers_frame, weight=3)
        upper.add(host_frame, weight=2)

        self.container_tree = ttk.Treeview(
            containers_frame,
            columns=("service", "status", "health", "memory", "cpu"),
            show="headings",
            height=7,
        )
        headings = {
            "service": ("Service", 105),
            "status": ("Status", 75),
            "health": ("Health", 75),
            "memory": ("Memory", 150),
            "cpu": ("CPU", 70),
        }
        for column, (label, width) in headings.items():
            self.container_tree.heading(column, text=label)
            self.container_tree.column(column, width=width, anchor="center")
        self.container_tree.pack(fill="both", expand=True)

        container_actions = ttk.Frame(containers_frame)
        container_actions.pack(fill="x", pady=(6, 0))
        ttk.Combobox(
            container_actions,
            textvariable=self.container_service_var,
            values=tuple(CONTAINERS),
            state="readonly",
            width=12,
        ).pack(side="left", padx=2)
        ttk.Button(container_actions, text="Start", command=self.start_selected_container).pack(
            side="left", padx=2
        )
        ttk.Button(container_actions, text="Stop", command=self.stop_selected_container).pack(
            side="left", padx=2
        )
        ttk.Button(container_actions, text="Build Spark", command=self.build_spark).pack(
            side="left", padx=2
        )

        self.host_tree = ttk.Treeview(
            host_frame,
            columns=("application", "status", "port"),
            show="headings",
            height=7,
        )
        for column, label, width in (
            ("application", "Application", 135),
            ("status", "Status", 135),
            ("port", "Port", 55),
        ):
            self.host_tree.heading(column, text=label)
            self.host_tree.column(column, width=width, anchor="center")
        self.host_tree.pack(fill="both", expand=True)

        host_buttons = ttk.Frame(host_frame)
        host_buttons.pack(fill="x", pady=(6, 0))
        for index, key in enumerate(("meter", "tariff", "api", "dashboard")):
            label = self.host.specs[key].label
            button = ttk.Menubutton(host_buttons, text=label)
            menu = tk.Menu(button, tearoff=False)
            menu.add_command(label="Start", command=lambda value=key: self.start_host(value))
            menu.add_command(label="Stop", command=lambda value=key: self.stop_host(value))
            button["menu"] = menu
            button.grid(row=index // 2, column=index % 2, sticky="ew", padx=2, pady=2)
        host_buttons.columnconfigure(0, weight=1)
        host_buttons.columnconfigure(1, weight=1)

        middle = ttk.Frame(parent)
        middle.pack(fill="x", pady=7)

        airflow = ttk.LabelFrame(
            middle, text="Airflow", padding=6, style="Section.TLabelframe"
        )
        airflow.pack(side="left", fill="x", expand=True, padx=(0, 4))
        ttk.Combobox(
            airflow,
            textvariable=self.dag_var,
            values=("tariff_ingestion", "daily_billing"),
            state="readonly",
            width=20,
        ).pack(side="left", padx=2)
        ttk.Button(airflow, text="Unpause both", command=self.unpause_dags).pack(
            side="left", padx=2
        )
        ttk.Button(airflow, text="Trigger selected", command=self.trigger_dag).pack(
            side="left", padx=2
        )
        ttk.Button(airflow, text="Show password", command=self.show_airflow_password).pack(
            side="left", padx=2
        )

        links = ttk.LabelFrame(
            middle, text="Open interfaces", padding=6, style="Section.TLabelframe"
        )
        links.pack(side="left", fill="x", expand=True, padx=(4, 0))
        for label, url in (
            ("Airflow", "http://localhost:8080"),
            ("Swagger", "http://127.0.0.1:8000/docs"),
            ("Dashboard", "http://127.0.0.1:8501"),
        ):
            ttk.Button(links, text=label, command=lambda value=url: webbrowser.open(value)).pack(
                side="left", padx=3
            )

        pipeline = ttk.LabelFrame(
            parent,
            text="Daily pipeline readiness",
            padding=6,
            style="Section.TLabelframe",
        )
        pipeline.pack(fill="both", expand=True)
        self.pipeline_tree = ttk.Treeview(
            pipeline,
            columns=("date", "energy", "tariff", "energy_count", "tariff_count", "billing", "error"),
            show="headings",
            height=9,
        )
        pipeline_columns = (
            ("date", "Date", 90),
            ("energy", "Energy ready", 85),
            ("tariff", "Tariff ready", 85),
            ("energy_count", "Energy HH", 75),
            ("tariff_count", "Tariff HH", 75),
            ("billing", "Billing", 90),
            ("error", "Last error", 330),
        )
        for column, label, width in pipeline_columns:
            self.pipeline_tree.heading(column, text=label)
            self.pipeline_tree.column(column, width=width, anchor="w" if column == "error" else "center")
        self.pipeline_tree.pack(fill="both", expand=True)

    def _build_logs(self, parent: ttk.Frame) -> None:
        controls = ttk.Frame(parent)
        controls.pack(fill="x", pady=(0, 6))
        ttk.Label(controls, text="Source:").pack(side="left")
        ttk.Combobox(
            controls,
            textvariable=self.log_source_var,
            values=("postgres", "kafka", "spark", "airflow", "meter", "tariff", "api", "dashboard", "system"),
            state="readonly",
            width=18,
        ).pack(side="left", padx=5)
        ttk.Button(controls, text="Refresh logs", command=self.refresh_logs).pack(side="left")
        ttk.Button(controls, text="Clear view", command=self._clear_log_view).pack(
            side="left", padx=5
        )
        ttk.Label(
            controls,
            text="Host logs are bounded to the latest 750 lines.",
            foreground="#555555",
        ).pack(side="right")

        self.log_text = scrolledtext.ScrolledText(
            parent,
            wrap="word",
            font=("Consolas", 9),
            state="disabled",
        )
        self.log_text.pack(fill="both", expand=True)

    def _post(self, callback: object, *args: object) -> None:
        self.ui_queue.put((callback, args))

    def _drain_ui_queue(self) -> None:
        try:
            while True:
                callback, args = self.ui_queue.get_nowait()
                callback(*args)  # type: ignore[operator]
        except Empty:
            pass
        self.root.after(100, self._drain_ui_queue)

    def _set_status(self, text: str) -> None:
        self.status_var.set(text)

    def _append_system_log(self, text: str) -> None:
        self._append_log("system", text)

    def _append_log(self, source: str, line: str) -> None:
        if self.log_source_var.get() != source:
            return
        self.log_text.configure(state="normal")
        self.log_text.insert("end", line.rstrip() + "\n")
        self.log_text.see("end")
        self.log_text.configure(state="disabled")

    def _replace_log(self, text: str) -> None:
        self.log_text.configure(state="normal")
        self.log_text.delete("1.0", "end")
        self.log_text.insert("1.0", text)
        self.log_text.see("end")
        self.log_text.configure(state="disabled")

    def _clear_log_view(self) -> None:
        self._replace_log("")

    def _run_action(self, label: str, worker: object) -> None:
        if not self.action_lock.acquire(blocking=False):
            messagebox.showinfo("Operation in progress", "Wait for the current operation to finish.")
            return

        self._set_status(f"{label}...")

        def target() -> None:
            try:
                result = worker()  # type: ignore[operator]
                if result:
                    self._post(self._append_system_log, str(result))
                self._post(self._set_status, f"{label}: completed.")
            except Exception as error:  # UI boundary: display controlled error.
                self._post(self._set_status, f"{label}: failed.")
                self._post(messagebox.showerror, label, str(error))
                self._post(self._append_system_log, f"ERROR: {error}")
            finally:
                self.action_lock.release()
                self._post(self.refresh_async)

        threading.Thread(target=target, daemon=True).start()

    def refresh_async(self) -> None:
        if not self.refresh_lock.acquire(blocking=False):
            return

        def worker() -> None:
            try:
                engine = self.docker.engine_info()
                states = self.docker.container_states()
                stats = self.docker.resource_stats()
                pipeline = fetch_pipeline_status()
                host_states = {
                    key: self.host.status(key)
                    for key in self.host.specs
                }
                self._post(self._apply_refresh, engine, states, stats, pipeline, host_states)
            except Exception as error:
                self._post(self.engine_var.set, "Docker: unavailable")
                self._post(self.memory_var.set, "Memory: unavailable")
                self._post(self._set_status, f"Status refresh failed: {error}")
            finally:
                self.refresh_lock.release()
                self._post(self._schedule_refresh)

        threading.Thread(target=worker, daemon=True).start()

    def _schedule_refresh(self) -> None:
        if self.refresh_after_id is not None:
            self.root.after_cancel(self.refresh_after_id)
        self.refresh_after_id = self.root.after(REFRESH_MS, self.refresh_async)

    def _apply_refresh(
        self,
        engine: dict[str, object],
        states: dict[str, dict[str, str]],
        stats: dict[str, dict[str, str]],
        pipeline: list[dict[str, object]],
        host_states: dict[str, str],
    ) -> None:
        self.engine_var.set(f"Docker {engine['version']} · {engine['cpus']} CPUs")
        gib = int(engine["memory_bytes"]) / (1024 ** 3)
        self.memory_var.set(f"Docker allocation: {gib:.2f} GiB")

        for item in self.container_tree.get_children():
            self.container_tree.delete(item)
        for service, container in CONTAINERS.items():
            state = states[service]
            usage = stats.get(container, {})
            self.container_tree.insert(
                "",
                "end",
                values=(
                    service,
                    state["status"],
                    state["health"] or "—",
                    usage.get("memory", "—"),
                    usage.get("cpu", "—"),
                ),
            )

        for item in self.host_tree.get_children():
            self.host_tree.delete(item)
        for key, spec in self.host.specs.items():
            self.host_tree.insert(
                "",
                "end",
                values=(spec.label, host_states[key], spec.port or "—"),
            )

        for item in self.pipeline_tree.get_children():
            self.pipeline_tree.delete(item)
        for row in pipeline:
            self.pipeline_tree.insert(
                "",
                "end",
                values=(
                    row["energy_date"],
                    "Yes" if row["energy_ready"] else "No",
                    "Yes" if row["tariff_ready"] else "No",
                    row["energy_households"],
                    row["tariff_households"],
                    row["billing_status"],
                    row["last_error"],
                ),
            )

    def start_infrastructure(self) -> None:
        def worker() -> str:
            self._post(self._set_status, "Starting PostgreSQL and Kafka...")
            self.docker.compose_up("postgres", "kafka")
            self._post(self._set_status, "Waiting for PostgreSQL and Kafka health checks...")
            self.docker.wait_for_health(("postgres", "kafka"))
            return "PostgreSQL and Kafka are healthy."

        self._run_action("Start infrastructure", worker)

    def bootstrap(self) -> None:
        self._run_action("Bootstrap database and Kafka", self.docker.bootstrap)

    def start_compute(self) -> None:
        def worker() -> str:
            self.docker.compose_up("spark", "airflow")
            self._post(self._set_status, "Waiting for Airflow DAG discovery...")
            self.docker.wait_for_airflow()
            output = self.docker.unpause_dags()
            return output or "Spark and Airflow started; DAGs unpaused."

        self._run_action("Start Spark and Airflow", worker)

    def guided_full_start(self) -> None:
        proceed = messagebox.askyesno(
            "Guided full start",
            "This starts all four containers, both generators, FastAPI, and "
            "Streamlit. It may use most of Docker's memory, and the generators "
            "will write simulated data. Continue?",
        )
        if not proceed:
            return

        def worker() -> str:
            self._post(self._set_status, "Step 1/6: starting infrastructure...")
            self.docker.compose_up("postgres", "kafka")
            self._post(self._set_status, "Step 2/6: waiting for health checks...")
            self.docker.wait_for_health(("postgres", "kafka"))
            self._post(self._set_status, "Step 3/6: bootstrapping database and topic...")
            bootstrap_output = self.docker.bootstrap()
            self._post(self._set_status, "Step 4/6: starting Spark and Airflow...")
            self.docker.compose_up("spark", "airflow")
            self._post(self._set_status, "Step 5/6: waiting for Airflow...")
            self.docker.wait_for_airflow()
            self.docker.unpause_dags()
            self._post(self._set_status, "Step 6/6: starting host applications...")
            for key in ("tariff", "meter", "api", "dashboard"):
                if self.host.status(key) == "Stopped":
                    self.host.start(key, self._host_log_callback)
            return bootstrap_output + "\nFull system start completed."

        self._run_action("Guided full start", worker)

    def stop_all(self) -> None:
        if not messagebox.askyesno(
            "Stop all",
            "Stop host applications and remove project containers while preserving "
            "database volumes and Spark checkpoints?",
        ):
            return

        def worker() -> str:
            self.host.stop_all()
            self.docker.compose_down(remove_volumes=False)
            return "Stopped host applications and containers. Data volumes were preserved."

        self._run_action("Stop all", worker)

    def factory_reset(self) -> None:
        answer = simpledialog.askstring(
            "Factory reset",
            "This permanently deletes PostgreSQL data, Airflow metadata, and Spark "
            "checkpoints. Git-tracked tariff CSVs remain. Type RESET to continue:",
            parent=self.root,
        )
        if answer != "RESET":
            if answer is not None:
                messagebox.showinfo("Factory reset", "Reset cancelled; RESET was not entered.")
            return

        def worker() -> str:
            self.host.stop_all()
            self.docker.compose_down(remove_volumes=True)
            return "Factory reset completed. Repository files were not deleted."

        self._run_action("Factory reset", worker)

    def start_selected_container(self) -> None:
        service = self.container_service_var.get()
        self._run_action(f"Start {service}", lambda: self.docker.compose_up(service).output)

    def stop_selected_container(self) -> None:
        service = self.container_service_var.get()
        self._run_action(f"Stop {service}", lambda: self.docker.compose_stop(service).output)

    def build_spark(self) -> None:
        if not messagebox.askyesno(
            "Build Spark image",
            "Rebuild the Spark image now? This is only needed after Dockerfile changes.",
        ):
            return
        self._run_action("Build Spark image", lambda: self.docker.build_spark().output)

    def start_host(self, key: str) -> None:
        self._run_action(
            f"Start {self.host.specs[key].label}",
            lambda: f"Started PID {self.host.start(key, self._host_log_callback).process.pid}.",
        )

    def stop_host(self, key: str) -> None:
        def worker() -> str:
            stopped = self.host.stop(key)
            return "Stopped." if stopped else "The control center does not own a running process."

        self._run_action(f"Stop {self.host.specs[key].label}", worker)

    def _host_log_callback(self, key: str, line: str) -> None:
        self._post(self._append_log, key, line)

    def unpause_dags(self) -> None:
        self._run_action("Unpause Airflow DAGs", self.docker.unpause_dags)

    def trigger_dag(self) -> None:
        dag_id = self.dag_var.get()
        self._run_action(f"Trigger {dag_id}", lambda: self.docker.trigger_dag(dag_id).output)

    def show_airflow_password(self) -> None:
        def worker() -> str:
            result = self.docker.runner.run(
                [
                    "docker",
                    "exec",
                    CONTAINERS["airflow"],
                    "cat",
                    "/opt/airflow/simple_auth_manager_passwords.json.generated",
                ]
            )
            if not result.ok:
                raise CommandError(result.output or "Airflow password file is unavailable.")
            return result.stdout

        self.log_source_var.set("system")
        self._run_action("Read Airflow login file", worker)

    def refresh_logs(self) -> None:
        source = self.log_source_var.get()
        if source == "system":
            return
        if source in self.host.specs:
            self._replace_log(self.host.log_text(source))
            return

        def worker() -> str:
            text = self.docker.logs(source)
            self._post(self._replace_log, text)
            return ""

        self._run_action(f"Load {source} logs", worker)

    def _on_close(self) -> None:
        answer = messagebox.askyesnocancel(
            "Close Control Center",
            "Leave containers and host applications running?\n\n"
            "Yes: close only this UI.\n"
            "No: stop host applications started by this UI, then close.\n"
            "Cancel: return to the UI.",
        )
        if answer is None:
            return
        if answer is False:
            self.host.stop_all()
        self.root.destroy()


def main() -> None:
    root = tk.Tk()
    ControlCenter(root)
    root.mainloop()


if __name__ == "__main__":
    main()
