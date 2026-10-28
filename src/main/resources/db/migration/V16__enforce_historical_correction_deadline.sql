CREATE FUNCTION check_correction_deadline()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    incident_status VARCHAR(10);
    correction_deadline TIMESTAMP;
BEGIN
    SELECT status
    INTO incident_status
    FROM incident
    WHERE incident_id = NEW.incident_id;

    IF incident_status = 'CLOSED' THEN
        correction_deadline := NULLIF(
            current_setting('failureintel.historical_correction_deadline', true), '')::TIMESTAMP;
        IF correction_deadline IS NULL THEN
            RAISE EXCEPTION 'historical correction deadline is required'
                USING ERRCODE = '23514';
        END IF;
        IF clock_timestamp() >= correction_deadline AT TIME ZONE 'UTC' THEN
            DELETE FROM incident_membership WHERE event_id = NEW.event_id;
            UPDATE incident SET associated_event_count = associated_event_count - 1
                WHERE incident_id = NEW.incident_id;
            UPDATE grouping_work
            SET status = 'COMPLETED', processing_started_at = NULL, next_attempt_at = NULL,
                terminal_outcome = 'UNASSIGNED', unassigned_reason = 'HISTORICAL_CORRECTION_EXPIRED',
                failure_code = NULL, failure_reason = NULL
            WHERE event_id = NEW.event_id;
        END IF;
    END IF;

    RETURN NULL;
END;
$$;

CREATE CONSTRAINT TRIGGER trg_historical_correction_deadline
    AFTER INSERT ON incident_membership
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW
    EXECUTE FUNCTION check_correction_deadline();
