-- Drop indexes on old table if they exist
DROP INDEX IF EXISTS idx_failure_event_service_name;
DROP INDEX IF EXISTS idx_failure_event_environment;
DROP INDEX IF EXISTS idx_failure_event_occurred_at;

-- Rename old scaffolded table
ALTER TABLE IF EXISTS failure_event RENAME TO failure_event_old;

-- Create the correct failure_event table with all required columns
CREATE TABLE failure_event (
    event_id UUID PRIMARY KEY,
    occurred_at TIMESTAMP NOT NULL,
    failure_type VARCHAR(255),
    ingested_at TIMESTAMP NOT NULL,
    service_name VARCHAR(120) NOT NULL,
    server_name VARCHAR(255),
    environment VARCHAR(50) NOT NULL,
    event_type VARCHAR(80) NOT NULL,
    error_type VARCHAR(150),
    message TEXT,
    dependency_target VARCHAR(200),
    trace_id VARCHAR(120),
    severity_hint VARCHAR(50),
    raw_payload TEXT,
    normalized_payload TEXT,
    processing_status VARCHAR(20) NOT NULL DEFAULT 'RECEIVED',
    incident_id UUID
);

-- Copy data from old table if it exists and has data
INSERT INTO failure_event (event_id, occurred_at, service_name, environment, event_type, error_type, message)
SELECT id, occurred_at, service_name, environment, NULL, error_type, message
FROM failure_event_old
ON CONFLICT (event_id) DO NOTHING;

-- Drop old table
DROP TABLE IF EXISTS failure_event_old CASCADE;

-- Create indexes for common queries
CREATE INDEX idx_trace_id ON failure_event(trace_id);
CREATE INDEX idx_service_name ON failure_event(service_name);
CREATE INDEX idx_environment ON failure_event(environment);
CREATE INDEX idx_processing_status ON failure_event(processing_status);
CREATE INDEX idx_occurred_at ON failure_event(occurred_at);
