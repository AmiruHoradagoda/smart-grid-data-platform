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
