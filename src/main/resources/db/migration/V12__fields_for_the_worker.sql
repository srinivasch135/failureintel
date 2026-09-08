ALTER TABLE failure_event
    ADD COLUMN source_metadata JSONB,
    ADD COLUMN attempt_count INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN last_attempt_at TIMESTAMP,
    ADD COLUMN next_attempt_at TIMESTAMP,
    ADD COLUMN processing_started_at TIMESTAMP,
    ADD COLUMN failure_code VARCHAR(80);

CREATE INDEX idx_failure_event_processing_queue
    ON failure_event (
        processing_status,
        next_attempt_at,
        ingested_at
    );