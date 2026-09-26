# Demo checklist (5–10 minutes)

Prepare dependencies with `uv sync` and run `uv run pytest` before presenting.
Use existing data for a reliable, read-only demo. Have FastAPI running with
`uv run uvicorn src.api.main:app --reload` and start the dashboard in a separate terminal with
`uv run streamlit run dashboard/app.py`. Open http://localhost:8501.

- [ ] **0–1 min: Architecture.** Show the README diagram and explain streaming
  versus batch processing, joined on household/date for billing.
- [ ] **1–2 min: Containers and Kafka.** Run `docker compose ps`, then inspect
  the topic:
  ```powershell
  docker exec smart-grid-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 --describe --topic smart-meter-readings
  docker exec smart-grid-kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:29092 --topic smart-meter-readings --from-beginning --max-messages 1 --timeout-ms 10000
  ```
  Explain the meter producer command below. Start it only for a fresh demo
  dataset or continue an already-running producer; restarting resets event time.
  ```powershell
  uv run python -m src.producers.smart_meter_producer
  ```
- [ ] **2–3 min: Spark.** Run `docker compose logs --tail=20 spark`. Explain
  five-minute windows, the watermark, and finalized daily summaries.
- [ ] **3-4 min: Airflow.** Show tariff ingestion and daily billing DAG status
  at http://localhost:8080. Show an existing tariff CSV, without regenerating it.
- [ ] **4-8 min: Dashboard.** Open http://localhost:8501 after starting:
  ```powershell
  uv run streamlit run dashboard/app.py
  ```
  Show API health, energy KPI cards, Energy Flow Over Time, zone energy balance,
  renewable contribution, operational insights, and API-provided alerts.
  Explain simulated window timestamps and any partial history warning.
  Select January 1, 2026 for household billing, compare tariff-tier averages,
  and show the billing table. Try a missing date to show the friendly empty state.
  Use Refresh Dashboard to request fresh results.
- [ ] **8-9 min: API.** Briefly show `/health` or Swagger at
  http://127.0.0.1:8000/docs. Explain Streamlit reads FastAPI; it does not access
  PostgreSQL or implement separate billing/alert decisions.
- [ ] **9-10 min: Tests.** Run `uv run pytest`; explain mocked database tests,
  alert boundaries, tariff reproducibility, and bounded history requests.

For a fresh live demonstration, start both generators together before presenting
and allow more than five minutes plus Airflow scheduling time for billing.
Never reset volumes, checkpoints, metadata, or CSVs as a demo shortcut.
