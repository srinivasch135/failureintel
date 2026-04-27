CREATE TABLE failure_event (
    id UUID PRIMARY KEY,
    service_name VARCHAR(255),
    environment VARCHAR(50),
    error_type VARCHAR(255),
    message TEXT,
    occurred_at TIMESTAMP
);