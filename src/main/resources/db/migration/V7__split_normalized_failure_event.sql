CREATE TABLE normalized_failure_event(
     event_id UUID PRIMARY KEY,
    normalized_payload JSONB,
    normalized_service_name VARCHAR(255),
    normalized_environment VARCHAR(100),
    normalized_event_type VARCHAR(100),
    normalized_error_type VARCHAR(255),
    normalized_error_message TEXT,
    normalized_dependency_target VARCHAR(255),
    normalized_trace_id VARCHAR(255),
    normalized_severity VARCHAR(50),
    normalized_occurred_at TIMESTAMP,
    normalization_status VARCHAR(50) NOT NULL DEFAULT 'PARTIALLY_NORMALIZED',
    normalization_metadata JSONB,
    failure_reason TEXT,
    normalized_at TIMESTAMP,
    created_at TIMESTAMP NOT NULL DEFAULT now(),
    updated_at TIMESTAMP NOT NULL DEFAULT now(),

    CONSTRAINT fk_normalized_failure_event
    FOREIGN KEY (event_id)
    REFERENCES failure_event(event_id)
    ON DELETE CASCADE
);
