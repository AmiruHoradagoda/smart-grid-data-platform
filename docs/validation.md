# Validation commands

Run from the repository root in PowerShell. These checks do not insert/update
business data or trigger DAGs. Existing background jobs may continue normal work.

## Containers and Kafka

```powershell
docker compose config --quiet
docker compose ps
docker exec smart-grid-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 --describe --topic smart-meter-readings
docker compose logs --tail=30 spark
```

Expect a healthy broker/database and running Spark/Airflow. Spark initially
downloads Java connectors. Check logs for exceptions, not just container uptime.

## PostgreSQL

```powershell
docker exec -it smart-grid-postgres psql -U smartgrid -d smart_grid
```

Run these read-only queries:

```sql
SELECT COUNT(*) FROM zone_energy_metrics;
SELECT energy_date, COUNT(*) AS households FROM household_daily_energy GROUP BY energy_date ORDER BY energy_date;
SELECT effective_date, COUNT(*) AS households FROM tariff_reference GROUP BY effective_date ORDER BY effective_date;
SELECT energy_date, COUNT(*) AS households FROM daily_household_billing GROUP BY energy_date ORDER BY energy_date;

-- Expected zero: stored bills disagree with current matching input rows.
SELECT COUNT(*) AS mismatched_bills
FROM daily_household_billing b
JOIN household_daily_energy e USING (energy_date, household_id)
JOIN tariff_reference t ON t.household_id = e.household_id AND t.effective_date = e.energy_date
WHERE b.estimated_bill_lkr <> ROUND((e.total_grid_import_kwh * t.tariff_rate)::numeric, 2);

-- Expected zero: renewable ratios disagree with the implemented formula.
-- Tiny tolerance accommodates storage as double precision.
SELECT COUNT(*) AS mismatched_ratios FROM zone_energy_metrics
WHERE ABS(renewable_contribution_pct - CASE WHEN total_consumption_kwh > 0
THEN ROUND((total_solar_generation_kwh / total_consumption_kwh * 100)::numeric, 2)
ELSE 0 END) > 0.000001;
```

Each complete day should normally have 20 households. A missing latest day may
still be waiting for the event-time watermark or the next Airflow run. Exit with
`\q`. Host tools connect to `127.0.0.1:5433`; Docker uses `postgres:5432`.

## Airflow

```powershell
docker exec smart-grid-airflow airflow db check
docker exec smart-grid-airflow airflow jobs check --job-type SchedulerJob --local
docker exec smart-grid-airflow airflow dags list
docker exec smart-grid-airflow airflow dags list-import-errors
docker exec smart-grid-airflow airflow dags list-runs tariff_ingestion
docker exec smart-grid-airflow airflow dags list-runs daily_billing
```

Expect successful DB access, an alive scheduler, both DAGs, and no import errors.
New files may take up to the discovery interval to appear. Airflow commands run
inside Linux Docker, not the Windows virtual environment. `list-runs` takes the
DAG ID as a positional argument (no `-d`). Metadata is in `airflow_meta`.

## API

Start it in a separate terminal if needed:

```powershell
uv run uvicorn src.api.main:app --reload
```

```powershell
Invoke-RestMethod http://127.0.0.1:8000/health
Invoke-RestMethod http://127.0.0.1:8000/api/v1/zones/latest
Invoke-RestMethod 'http://127.0.0.1:8000/api/v1/billing/daily?billing_date=2026-01-01'
Invoke-RestMethod http://127.0.0.1:8000/api/v1/households/HH-001/billing
```

Choose a billing date present in SQL results. Missing data returns 404; invalid
dates return 422. Swagger is at http://127.0.0.1:8000/docs. Renewable alerts have
configuration only, so no successful alert response can currently be validated.

## Automated tests and import checks

```powershell
uv run pytest
uv run python -c "import src.api.main; import src.producers.smart_meter_producer; import src.producers.tariff_generator; import utils.config_loader; print('Imports OK')"
docker exec smart-grid-airflow python -c "from airflow.sdk import dag, task; import runpy; runpy.run_path('/opt/airflow/dags/tariff_ingestion_dag.py'); runpy.run_path('/opt/airflow/dags/daily_billing_dag.py'); print('DAG imports OK')"
```

Unit tests do not run Spark or billing SQL and do not require live services.
The SQL checks above validate stored results separately. No unused copies of
production formulas were added just to make a unit test pass.
