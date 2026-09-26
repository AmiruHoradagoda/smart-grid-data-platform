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