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