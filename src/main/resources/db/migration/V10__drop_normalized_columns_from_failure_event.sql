ALTER TABLE failure_event
DROP COLUMN IF EXISTS normalized_payload,
DROP COLUMN IF EXISTS normalized_service_name,
DROP COLUMN IF EXISTS normalized_environment,
DROP COLUMN IF EXISTS normalized_event_type,
DROP COLUMN IF EXISTS normalized_error_type,
DROP COLUMN IF EXISTS normalized_error_message,
DROP COLUMN IF EXISTS normalized_dependency_target,
DROP COLUMN IF EXISTS normalized_trace_id,
DROP COLUMN IF EXISTS normalized_severity,
DROP COLUMN IF EXISTS normalized_occurred_at,
DROP COLUMN IF EXISTS normalization_status,
DROP COLUMN IF EXISTS normalization_metadata;
