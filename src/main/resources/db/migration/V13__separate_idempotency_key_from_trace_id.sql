ALTER TABLE failure_event
    ADD COLUMN idempotency_key VARCHAR(255),
    ADD COLUMN ingestion_fingerprint VARCHAR(67);

-- Preserve the old trace-based duplicate behavior for rows written before
-- explicit idempotency keys were introduced. The namespace prevents a
-- producer key from colliding with a legacy trace fallback.
UPDATE failure_event
SET idempotency_key = 'trace:' || BTRIM(trace_id)
WHERE idempotency_key IS NULL
  AND trace_id IS NOT NULL
  AND BTRIM(trace_id) <> '';

DROP INDEX IF EXISTS uq_failure_event_trace_id;

CREATE UNIQUE INDEX uq_failure_event_idempotency_key
    ON failure_event ((BTRIM(idempotency_key)))
    WHERE idempotency_key IS NOT NULL
      AND BTRIM(idempotency_key) <> '';
