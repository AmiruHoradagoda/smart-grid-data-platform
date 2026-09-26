# Tariff Generator

This folder contains the daily tariff data simulator used by the Smart Grid Data Platform.

`tariff_generator.py` creates one CSV file for each simulated day. The file contains the tariff and billing profile used later by the Airflow billing pipeline.

## Purpose

Smart meter data arrives continuously through Kafka, but tariff data is treated as **daily reference data**.

The generator simulates this behaviour:

```text
Simulated Day
    ↓
Generate tariff CSV
    ↓
data/tariffs/
    ↓
Airflow will process it later
```

This gives the project two different data sources:

```text
Streaming source   -> Smart meter readings
Daily batch source -> Tariff reference data
```

## Output File

A file is generated for every simulated day.

Example:

```text
data/tariffs/tariffs_2026-01-01.csv
data/tariffs/tariffs_2026-01-02.csv
data/tariffs/tariffs_2026-01-03.csv
```

Each file contains one row per household.

Example:

```csv
household_id,tariff_rate,billing_tier,subsidy_flag,effective_date
HH-001,42.0,STANDARD,false,2026-01-01
HH-002,32.0,SUBSIDIZED,true,2026-01-01
HH-003,48.0,PREMIUM,false,2026-01-01
```

## Household Tariff Profiles

Each household receives a billing profile.

Current billing tiers are:

| Billing Tier | Tariff Rate | Subsidy |
|---|---:|---|
| `SUBSIDIZED` | 32 LKR/kWh | Yes |
| `STANDARD` | 42 LKR/kWh | No |
| `PREMIUM` | 48 LKR/kWh | No |

These values come from:

```text
config/config.yaml
```

They are simulation values and are **not real electricity tariff rates**.

## Fixed Household Distribution

The project uses a fixed number of households for each tariff tier.

Example:

```text
SUBSIDIZED = 4 households
STANDARD   = 12 households
PREMIUM = 4 households
```

Total:

```text
4 + 12 + 4 = 20 households
```

The generator checks that:

```text
total tier household count
=
NUMBER_OF_HOUSEHOLDS
```

If they do not match, the application raises an error.

### Why?

This prevents incorrect configuration where some households would not receive a tariff profile.

## Reproducible assignment

`tariff.random_seed: 42` seeds a local random generator. The same household IDs
receive the same tariff tiers across restarts. This is separate from the solar
selection seed. Existing CSVs are not rewritten by this configuration change.
Running the generator again will overwrite files for matching dates.

## Tariff Rate Selection

The tariff rate is selected from the household's billing tier.

```text
If tier = SUBSIDIZED
    tariff_rate = 32

If tier = STANDARD
    tariff_rate = 42

If tier = PREMIUM
    tariff_rate = 48
```

The rate comes from `config.yaml`, not from hard-coded values inside the main logic.

### Why?

This separates business configuration from application logic, so rates can be changed without modifying the Python code.

## Subsidy Flag

The subsidy flag indicates whether the household belongs to the subsidized billing tier.

```text
SUBSIDIZED -> subsidy_flag = true
STANDARD   -> subsidy_flag = false
PREMIUM -> subsidy_flag = false
```

### Purpose

This field can later be used when calculating household bills or generating reports.

## Simulated Time

The project uses an accelerated clock:

```text
5 real minutes = 1 simulated day
```

Therefore:

```text
300 real seconds = 1 simulated day
```

After generating one daily tariff file, the script waits for one simulated day before generating the next file.

Example:

```text
Start:
tariffs_2026-01-01.csv

After 5 real minutes:
tariffs_2026-01-02.csv
```

### Why?

Waiting for real days would make the project impossible to demonstrate in a short university demo.

## Effective Date

Each tariff row contains:

```text
effective_date
```

Example:

```text
2026-01-01
```

### Purpose

This tells the later billing pipeline which tariff data belongs to which simulated day.

It also allows tariff history to be stored later.

## Configuration

The generator reads configuration values through:

```text
utils/config_loader.py
```

Main configuration values include:

```text
number of households
random seed
simulated day duration
simulation start date
output directory
tariff tier names
tariff rates
household counts
subsidy flags
```

All configuration is kept in:

```text
config/config.yaml
```

## Current Logic

The generator follows this process:

```text
Load configuration
        ↓
Create tariff tier list
        ↓
Validate household count
        ↓
Shuffle tiers using fixed seed
        ↓
Assign one tier to each household
        ↓
Set tariff rate
        ↓
Set subsidy flag
        ↓
Generate daily CSV
        ↓
Wait one simulated day
        ↓
Generate next CSV
```

## Example Household Record

A generated household profile may be:

```text
household_id = HH-005
billing_tier = STANDARD
tariff_rate = 42.0
subsidy_flag = false
effective_date = 2026-01-01
```

Final CSV row:

```text
HH-005,42.0,STANDARD,false,2026-01-01
```

## Main Assumptions

- The tariff data is simulated.
- Tariff rates are not real utility rates.
- Every household receives exactly one billing tier.
- Total tariff-tier household counts must equal the total number of households.
- Tariff assignments are reproducible with `tariff.random_seed: 42`.
- The tariff profile remains the same between simulated days for now.
- One tariff CSV is generated per simulated day.
- Five real minutes represent one simulated day.
- Tariff changes over time are not modelled yet.
- The generator creates files; the implemented Airflow DAG loads all tariff CSVs.

## Implemented Downstream Pipeline

```text
Tariff CSV
    ↓
Apache Airflow
    ↓
Validate file
    ↓
Load tariff data
    ↓
PostgreSQL
    ↓
Daily household billing
```
