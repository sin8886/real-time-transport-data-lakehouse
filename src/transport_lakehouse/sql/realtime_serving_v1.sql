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
