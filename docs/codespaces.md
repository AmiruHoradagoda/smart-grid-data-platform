# Run in GitHub Codespaces

The repository includes a dev-container configuration for a 4-core, 16 GB
Codespace. It installs Python 3.12, Docker-in-Docker, Docker Compose, and `uv`.
The first build and image download can take several minutes.

## Create the Codespace

1. Push this repository and branch to GitHub.
2. On the repository page, choose **Code > Codespaces > Create codespace**.
3. Select a 4-core machine if GitHub asks for a machine type.
4. Wait for the `postCreateCommand` to install `uv` and project dependencies.

All commands below run in the Codespaces Bash terminal from the repository root.

## Start the infrastructure

Start PostgreSQL and Kafka first:

```bash
docker compose up -d postgres kafka
docker compose ps
```

Wait until both containers are healthy. Create the separate Airflow metadata
database only when it does not already exist:

```bash
docker exec smart-grid-postgres \
  psql -U smartgrid -d postgres -tAc \
  "SELECT 1 FROM pg_database WHERE datname = 'airflow_meta';" \
  | grep -q 1 \
  || docker exec smart-grid-postgres createdb -U smartgrid airflow_meta
```

Create the Kafka topic explicitly:

```bash
docker exec smart-grid-kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:29092 \
  --create --if-not-exists \
  --topic smart-meter-readings \
  --partitions 3 \
  --replication-factor 1
```

Build and start Spark and Airflow:

```bash
docker compose up -d --build spark airflow
docker compose ps
```

Allow time for Spark connector downloads and Airflow initialization. Inspect
startup problems with:

```bash
docker compose logs --tail=50 spark
docker compose logs --tail=50 airflow
```

## Enable the Airflow DAGs

```bash
docker exec smart-grid-airflow airflow dags list
docker exec smart-grid-airflow airflow dags unpause tariff_ingestion
docker exec smart-grid-airflow airflow dags unpause daily_billing
```

Airflow is available from the forwarded port **8080**. Its generated local-demo
password can be viewed inside the Codespace terminal with:

```bash
docker exec smart-grid-airflow \
  cat /opt/airflow/simple_auth_manager_passwords.json.generated
```

Do not publish this password or include it in screenshots.

## Run the host processes

Start each command in a separate Codespaces terminal.

Smart-meter producer:

```bash
uv run python -m src.producers.smart_meter_producer
```

The repository already contains tariff files for January 1-4, so the tariff
generator is optional for the first demonstration:

```bash
uv run python -m src.producers.tariff_generator
```

FastAPI:

```bash
uv run uvicorn src.api.main:app --host 0.0.0.0 --port 8000
```

Streamlit:

```bash
uv run streamlit run dashboard/app.py \
  --server.address 0.0.0.0 \
  --server.port 8501
```

Use the Codespaces **Ports** panel to open:

- Port 8000: FastAPI and `/docs`
- Port 8080: Airflow
- Port 8501: Streamlit dashboard

Keep these ports private unless a public demonstration explicitly requires a
public URL. Kafka and PostgreSQL do not need forwarded public ports.

## Validate and stop

Run the unit tests:

```bash
uv run pytest
```

Stop host processes with `Ctrl+C`, then stop the infrastructure without deleting
the database or checkpoints:

```bash
docker compose stop
```

Do not use `docker compose down -v` unless deleting all generated PostgreSQL,
Airflow, and Spark checkpoint data is intentional. Stop the Codespace itself when
finished so it no longer consumes compute quota.

The meter and tariff generators restart their simulated clocks from the configured
start date. For an existing demonstration dataset, use the stored data instead of
restarting the generators.
