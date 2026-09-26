# Demo checklist (5–10 minutes)

Prepare dependencies with `uv sync` and run `uv run pytest` before presenting.
Use existing data for a reliable, read-only demo. Have FastAPI running with
`uv run uvicorn src.api.main:app --reload` and open its `/docs` page.

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
- [ ] **3–4 min: PostgreSQL.** Show recent zone results and available daily dates:
  ```powershell
  docker exec smart-grid-postgres psql -U smartgrid -d smart_grid -c "SELECT grid_zone, window_end, renewable_contribution_pct FROM zone_energy_metrics ORDER BY window_end DESC LIMIT 3;"
  docker exec smart-grid-postgres psql -U smartgrid -d smart_grid -c "SELECT energy_date, COUNT(*) FROM household_daily_energy GROUP BY energy_date ORDER BY energy_date;"
  ```
- [ ] **4–5 min: Tariffs and Airflow.** Show `Get-ChildItem data/tariffs` and
  `Get-Content data/tariffs/tariffs_2026-01-01.csv -TotalCount 5`. Show both DAGs
  in http://localhost:8080, their schedules, and successful run history.
  The generator command is `uv run python -m src.producers.tariff_generator`;
  do not rerun it against existing files during the read-only demo.
- [ ] **5–6 min: Reconciliation and billing.**
  ```powershell
  docker exec smart-grid-postgres psql -U smartgrid -d smart_grid -c "SELECT effective_date, COUNT(*) FROM tariff_reference GROUP BY effective_date ORDER BY effective_date;"
  docker exec smart-grid-postgres psql -U smartgrid -d smart_grid -c "SELECT energy_date, household_id, total_grid_import_kwh, tariff_rate, estimated_bill_lkr FROM daily_household_billing ORDER BY energy_date, household_id LIMIT 5;"
  ```
- [ ] **6–8 min: API.** In http://127.0.0.1:8000/docs, run `/health`, latest
  zones, daily bills for a date shown above, and household `HH-001` history.
  Explain 404 for absent data and 422 for invalid dates.
- [ ] **8–9 min: Alert scope.** Show the 20% / 06:00–18:00 settings in YAML.
  Explain why nights would be ignored. Clearly state the alert endpoint is
  not implemented; do not present it as a working feature.
- [ ] **9–10 min: Tests.** Run `uv run pytest`; explain mocked database tests,
  temporary CSV output, and the explicit skipped alert test.

For a fresh live demonstration, start both generators together before presenting
and allow more than five minutes plus Airflow scheduling time for billing.
Never reset volumes, checkpoints, metadata, or CSVs as a demo shortcut.
