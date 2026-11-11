CREATE TABLE IF NOT EXISTS dashboard_active_vehicles (
    provider VARCHAR NOT NULL,
    vehicle_id VARCHAR NOT NULL,
    trip_id VARCHAR,
    route_id VARCHAR,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    speed DOUBLE PRECISION,
    bearing DOUBLE PRECISION,
    event_ts TIMESTAMPTZ NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (provider, vehicle_id)
);

CREATE INDEX IF NOT EXISTS dashboard_active_vehicles_event_idx
    ON dashboard_active_vehicles (event_ts DESC);

CREATE TABLE IF NOT EXISTS dashboard_route_delay_trends (
    window_start TIMESTAMPTZ NOT NULL,
    window_end TIMESTAMPTZ NOT NULL,
    provider VARCHAR NOT NULL,
    route_id VARCHAR NOT NULL,
    avg_delay_seconds DOUBLE PRECISION,
    event_count BIGINT NOT NULL,
    PRIMARY KEY (window_start, window_end, provider, route_id)
);

CREATE INDEX IF NOT EXISTS dashboard_route_delay_trends_window_idx
    ON dashboard_route_delay_trends (window_start DESC);
