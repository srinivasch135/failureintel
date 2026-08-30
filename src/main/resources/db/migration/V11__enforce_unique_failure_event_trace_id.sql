CREATE UNIQUE INDEX uq_failure_event_trace_id
ON failure_event ((BTRIM(trace_id)))
WHERE trace_id IS NOT NULL
  AND BTRIM(trace_id) <> '';
