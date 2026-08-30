INSERT INTO NORMALIZED_FAILURE_EVENT (
    event_id,
    normalized_payload,
    normalized_service_name,
    normalized_environment,
    normalized_event_type,
    normalized_error_type,
    normalized_error_message,
    normalized_dependency_target,
    normalized_trace_id,
    normalized_severity,
    normalized_occurred_at,
    normalization_status,
    normalization_metadata,
    failure_reason,
    normalized_at
)
SELECT
    event_id,
    normalized_payload::jsonb,
    normalized_service_name,
    normalized_environment,
    normalized_event_type,
    normalized_error_type,
    normalized_error_message,
    normalized_dependency_target,
    normalized_trace_id,
    normalized_severity,
    normalized_occurred_at,
    normalization_status,
    normalization_metadata,
    failure_reason,
    normalized_occurred_at
FROM failure_event
WHERE normalization_status IS NOT NULL
ON CONFLICT (event_id) DO NOTHING;