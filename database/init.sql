CREATE TABLE IF NOT EXISTS zone_energy_metrics (
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    grid_zone VARCHAR(20) NOT NULL,

    total_consumption_kwh DOUBLE PRECISION NOT NULL,
    total_solar_generation_kwh DOUBLE PRECISION NOT NULL,
    total_grid_import_kwh DOUBLE PRECISION NOT NULL,

    renewable_contribution_pct DOUBLE PRECISION NOT NULL,
    meter_readings BIGINT NOT NULL,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (
        window_start,
        window_end,
        grid_zone
    )
);

CREATE TABLE IF NOT EXISTS tariff_reference (
    household_id VARCHAR(20) NOT NULL,
    effective_date DATE NOT NULL,

    tariff_rate NUMERIC(10, 2) NOT NULL,
    billing_tier VARCHAR(30) NOT NULL,
    subsidy_flag BOOLEAN NOT NULL,

    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (
        household_id,
        effective_date
    )
);

CREATE TABLE IF NOT EXISTS household_daily_energy (
    energy_date DATE NOT NULL,
    household_id VARCHAR(20) NOT NULL,
    grid_zone VARCHAR(20) NOT NULL,

    total_consumption_kwh DOUBLE PRECISION NOT NULL,
    total_solar_generation_kwh DOUBLE PRECISION NOT NULL,
    total_grid_import_kwh DOUBLE PRECISION NOT NULL,

    meter_readings BIGINT NOT NULL,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (
        energy_date,
        household_id
    )
);

CREATE TABLE IF NOT EXISTS daily_household_billing (
    energy_date DATE NOT NULL,
    household_id VARCHAR(20) NOT NULL,
    grid_zone VARCHAR(20) NOT NULL,

    total_consumption_kwh DOUBLE PRECISION NOT NULL,
    total_solar_generation_kwh DOUBLE PRECISION NOT NULL,
    total_grid_import_kwh DOUBLE PRECISION NOT NULL,

    tariff_rate NUMERIC(10, 2) NOT NULL,
    billing_tier VARCHAR(30) NOT NULL,
    subsidy_flag BOOLEAN NOT NULL,

    estimated_bill_lkr NUMERIC(12, 2) NOT NULL,

    calculated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (
        energy_date,
        household_id
    )
);

CREATE TABLE IF NOT EXISTS daily_pipeline_status (
    energy_date DATE PRIMARY KEY,

    energy_ready BOOLEAN NOT NULL DEFAULT FALSE,
    tariff_ready BOOLEAN NOT NULL DEFAULT FALSE,

    energy_households INTEGER NOT NULL DEFAULT 0
        CHECK (energy_households >= 0),
    tariff_households INTEGER NOT NULL DEFAULT 0
        CHECK (tariff_households >= 0),

    billing_status VARCHAR(20) NOT NULL DEFAULT 'PENDING'
        CHECK (
            billing_status IN (
                'PENDING',
                'PROCESSING',
                'COMPLETED',
                'FAILED'
            )
        ),

    energy_ready_at TIMESTAMP,
    tariff_ready_at TIMESTAMP,
    billing_started_at TIMESTAMP,
    billed_at TIMESTAMP,

    last_error TEXT,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_daily_pipeline_ready
    ON daily_pipeline_status (energy_date)
    WHERE energy_ready = TRUE
      AND tariff_ready = TRUE
      AND billing_status IN ('PENDING', 'FAILED');
