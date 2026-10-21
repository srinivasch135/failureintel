CREATE TABLE incident (
    incident_id UUID PRIMARY KEY,
    service_name VARCHAR(255) NOT NULL,
    environment VARCHAR(100) NOT NULL,
    error_type VARCHAR(255) NOT NULL,
    dependency_target VARCHAR(255),
    status VARCHAR(10) NOT NULL DEFAULT 'ACTIVE',
    first_occurred_at TIMESTAMP NOT NULL,
    last_occurred_at TIMESTAMP NOT NULL,
    inactivity_expires_at TIMESTAMP NOT NULL,
    closed_at TIMESTAMP,
    associated_event_count BIGINT NOT NULL DEFAULT 1,
    CONSTRAINT ck_incident_status CHECK (status IN ('ACTIVE', 'CLOSED')),
    CONSTRAINT ck_incident_occurrence_bounds CHECK (first_occurred_at <= last_occurred_at),
    CONSTRAINT ck_incident_associated_event_count CHECK (associated_event_count >= 1),
    CONSTRAINT ck_incident_closure_timestamp CHECK (
        (status = 'ACTIVE' AND closed_at IS NULL)
        OR (status = 'CLOSED' AND closed_at IS NOT NULL)
    )
);

CREATE INDEX idx_incident_signature
    ON incident (service_name, environment, error_type, dependency_target, status);

CREATE INDEX idx_incident_active_inactivity_expiry
    ON incident (inactivity_expires_at)
    WHERE status = 'ACTIVE';

CREATE TABLE incident_membership (
    event_id UUID PRIMARY KEY,
    incident_id UUID NOT NULL,
    CONSTRAINT fk_incident_membership_normalized_event
        FOREIGN KEY (event_id) REFERENCES normalized_failure_event(event_id),
    CONSTRAINT fk_incident_membership_incident
        FOREIGN KEY (incident_id) REFERENCES incident(incident_id)
);

CREATE INDEX idx_incident_membership_incident_id
    ON incident_membership (incident_id);

CREATE TABLE grouping_work (
    event_id UUID PRIMARY KEY,
    status VARCHAR(12) NOT NULL DEFAULT 'PENDING',
    attempt_count INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMP NOT NULL DEFAULT now(),
    last_attempt_at TIMESTAMP,
    processing_started_at TIMESTAMP,
    next_attempt_at TIMESTAMP,
    failure_code VARCHAR(80),
    failure_reason TEXT,
    terminal_outcome VARCHAR(12),
    unassigned_reason VARCHAR(80),
    CONSTRAINT fk_grouping_work_normalized_event
        FOREIGN KEY (event_id) REFERENCES normalized_failure_event(event_id),
    CONSTRAINT ck_grouping_work_status
        CHECK (status IN ('PENDING', 'PROCESSING', 'RETRYABLE', 'COMPLETED', 'FAILED')),
    CONSTRAINT ck_grouping_work_attempt_count CHECK (attempt_count >= 0),
    CONSTRAINT ck_grouping_work_processing_lease CHECK (
        (status = 'PROCESSING' AND processing_started_at IS NOT NULL AND last_attempt_at IS NOT NULL)
        OR (status <> 'PROCESSING' AND processing_started_at IS NULL)
    ),
    CONSTRAINT ck_grouping_work_retry_schedule CHECK (
        (status = 'RETRYABLE' AND next_attempt_at IS NOT NULL)
        OR (status <> 'RETRYABLE' AND next_attempt_at IS NULL)
    ),
    CONSTRAINT ck_grouping_work_terminal_outcome CHECK (
        (
            status = 'COMPLETED'
            AND terminal_outcome IS NOT NULL
            AND terminal_outcome IN ('ASSOCIATED', 'UNASSIGNED')
            AND (
                (terminal_outcome = 'UNASSIGNED'
                    AND unassigned_reason IS NOT NULL
                    AND BTRIM(unassigned_reason) <> '')
                OR (terminal_outcome = 'ASSOCIATED' AND unassigned_reason IS NULL)
            )
        )
        OR (
            status <> 'COMPLETED'
            AND terminal_outcome IS NULL
            AND unassigned_reason IS NULL
        )
    )
);

CREATE INDEX idx_grouping_work_pending_queue
    ON grouping_work (created_at, event_id)
    WHERE status = 'PENDING';

CREATE INDEX idx_grouping_work_retryable_queue
    ON grouping_work (next_attempt_at, created_at, event_id)
    WHERE status = 'RETRYABLE';

CREATE INDEX idx_grouping_work_processing_recovery
    ON grouping_work (processing_started_at, event_id)
    WHERE status = 'PROCESSING';

CREATE FUNCTION lock_grouping_work_for_membership_change()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    affected_event_id UUID;
    event_ids UUID[];
BEGIN
    IF TG_OP = 'INSERT' THEN
        event_ids := ARRAY[NEW.event_id];
    ELSIF TG_OP = 'DELETE' THEN
        event_ids := ARRAY[OLD.event_id];
    ELSE
        event_ids := ARRAY[OLD.event_id, NEW.event_id];
    END IF;

    FOR affected_event_id IN
        SELECT DISTINCT ids.event_id
        FROM unnest(event_ids) AS ids(event_id)
        ORDER BY ids.event_id
    LOOP
        PERFORM 1
        FROM grouping_work
        WHERE event_id = affected_event_id
        FOR UPDATE;

        IF NOT FOUND THEN
            RAISE EXCEPTION 'grouping_work row is required for incident membership event %', affected_event_id
                USING ERRCODE = '23503';
        END IF;
    END LOOP;

    IF TG_OP = 'DELETE' THEN
        RETURN OLD;
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER trg_incident_membership_lock_grouping_work
    BEFORE INSERT OR UPDATE OR DELETE ON incident_membership
    FOR EACH ROW
    EXECUTE FUNCTION lock_grouping_work_for_membership_change();

CREATE FUNCTION assert_grouping_terminal_outcome(p_event_id UUID)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    work_status VARCHAR(12);
    work_outcome VARCHAR(12);
    has_membership BOOLEAN;
    has_work BOOLEAN;
BEGIN
    SELECT status, terminal_outcome
    INTO work_status, work_outcome
    FROM grouping_work
    WHERE event_id = p_event_id;
    has_work := FOUND;

    SELECT EXISTS (
        SELECT 1 FROM incident_membership WHERE event_id = p_event_id
    ) INTO has_membership;

    IF NOT has_work THEN
        IF has_membership THEN
            RAISE EXCEPTION 'incident membership has no grouping_work row for event %', p_event_id
                USING ERRCODE = '23514';
        END IF;
        RETURN;
    END IF;

    IF work_status = 'COMPLETED' AND work_outcome = 'ASSOCIATED' THEN
        IF NOT has_membership THEN
            RAISE EXCEPTION 'completed ASSOCIATED grouping_work requires incident membership for event %', p_event_id
                USING ERRCODE = '23514';
        END IF;
    ELSIF work_status = 'COMPLETED' AND work_outcome = 'UNASSIGNED' THEN
        IF has_membership THEN
            RAISE EXCEPTION 'completed UNASSIGNED grouping_work cannot have incident membership for event %', p_event_id
                USING ERRCODE = '23514';
        END IF;
    ELSIF has_membership THEN
        RAISE EXCEPTION 'incident membership requires completed ASSOCIATED grouping_work for event %', p_event_id
            USING ERRCODE = '23514';
    END IF;
END;
$$;

CREATE FUNCTION validate_grouping_terminal_outcome()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    affected_event_id UUID;
    event_ids UUID[];
BEGIN
    IF TG_OP = 'INSERT' THEN
        event_ids := ARRAY[NEW.event_id];
    ELSIF TG_OP = 'DELETE' THEN
        event_ids := ARRAY[OLD.event_id];
    ELSE
        event_ids := ARRAY[OLD.event_id, NEW.event_id];
    END IF;

    FOR affected_event_id IN
        SELECT DISTINCT ids.event_id
        FROM unnest(event_ids) AS ids(event_id)
        ORDER BY ids.event_id
    LOOP
        PERFORM assert_grouping_terminal_outcome(affected_event_id);
    END LOOP;

    RETURN NULL;
END;
$$;

CREATE CONSTRAINT TRIGGER trg_grouping_work_terminal_outcome
    AFTER INSERT OR UPDATE OR DELETE ON grouping_work
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW
    EXECUTE FUNCTION validate_grouping_terminal_outcome();

CREATE CONSTRAINT TRIGGER trg_incident_membership_terminal_outcome
    AFTER INSERT OR UPDATE OR DELETE ON incident_membership
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW
    EXECUTE FUNCTION validate_grouping_terminal_outcome();
