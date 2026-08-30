ALTER TABLE failure_event
ADD COLUMN normalized_service_name VARCHAR(255),
ADD COLUMN normalized_environment VARCHAR(100),
ADD COLUMN normalized_event_type VARCHAR(100),
ADD COLUMN normalized_error_type VARCHAR(255),
ADD COLUMN normalized_error_message TEXT,
ADD COLUMN normalized_dependency_target VARCHAR(255),
ADD COLUMN normalized_trace_id VARCHAR(255),
ADD COLUMN normalized_severity VARCHAR(50),
ADD COLUMN normalized_occurred_at TIMESTAMP,
ADD COLUMN normalization_status VARCHAR(50) NOT NULL DEFAULT 'PARTIALLY_NORMALIZED';
