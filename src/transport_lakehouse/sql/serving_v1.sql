CREATE TABLE IF NOT EXISTS dim_route (
    provider VARCHAR NOT NULL,
    route_id VARCHAR NOT NULL,
    agency_id VARCHAR,
    route_short_name VARCHAR,
    route_long_name VARCHAR,
    route_type INTEGER,
    CONSTRAINT dim_route_pkey PRIMARY KEY (provider, route_id)
);

CREATE TABLE IF NOT EXISTS dim_stop (
    provider VARCHAR NOT NULL,
    stop_id VARCHAR NOT NULL,
    stop_name VARCHAR,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    zone_id VARCHAR,
    wheelchair_boarding INTEGER,
    CONSTRAINT dim_stop_pkey PRIMARY KEY (provider, stop_id)
);

CREATE TABLE IF NOT EXISTS daily_metrics (
    provider VARCHAR NOT NULL,
    service_date DATE NOT NULL,
    scheduled_trip_count BIGINT NOT NULL CHECK (scheduled_trip_count >= 0),
    active_route_count BIGINT NOT NULL CHECK (active_route_count >= 0),
    active_stop_count BIGINT NOT NULL CHECK (active_stop_count >= 0),
    CONSTRAINT daily_metrics_pkey PRIMARY KEY (provider, service_date)
);

CREATE TABLE IF NOT EXISTS route_metrics (
    provider VARCHAR NOT NULL,
    service_date DATE NOT NULL,
    route_id VARCHAR NOT NULL,
    scheduled_trip_count BIGINT NOT NULL CHECK (scheduled_trip_count >= 0),
    stop_event_count BIGINT NOT NULL CHECK (stop_event_count >= 0),
    served_stop_count BIGINT NOT NULL CHECK (served_stop_count >= 0),
    CONSTRAINT route_metrics_pkey PRIMARY KEY (provider, service_date, route_id),
    CONSTRAINT route_metrics_dim_route_fkey
        FOREIGN KEY (provider, route_id)
        REFERENCES dim_route (provider, route_id)
);

CREATE TABLE IF NOT EXISTS stop_metrics (
    provider VARCHAR NOT NULL,
    service_date DATE NOT NULL,
    stop_id VARCHAR NOT NULL,
    scheduled_trip_count BIGINT NOT NULL CHECK (scheduled_trip_count >= 0),
    stop_event_count BIGINT NOT NULL CHECK (stop_event_count >= 0),
    CONSTRAINT stop_metrics_pkey PRIMARY KEY (provider, service_date, stop_id),
    CONSTRAINT stop_metrics_dim_stop_fkey
        FOREIGN KEY (provider, stop_id)
        REFERENCES dim_stop (provider, stop_id)
);

CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id UUID NOT NULL,
    pipeline_name VARCHAR NOT NULL,
    provider VARCHAR,
    ingest_date DATE,
    started_at TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ NOT NULL,
    duration_ms BIGINT NOT NULL CHECK (duration_ms >= 0),
    status VARCHAR NOT NULL CHECK (status IN ('success', 'skipped', 'failed')),
    records_in BIGINT NOT NULL CHECK (records_in >= 0),
    records_out BIGINT NOT NULL CHECK (records_out >= 0),
    failed_records BIGINT NOT NULL CHECK (failed_records >= 0),
    error_message TEXT,
    CONSTRAINT pipeline_runs_pkey PRIMARY KEY (run_id)
);
