UPDATE failure_event
SET normalization_status = 'FULLY_NORMALIZED'
WHERE normalization_status = 'NORMALIZED';

UPDATE failure_event
SET normalization_status = 'MALFORMED'
WHERE normalization_status = 'NORMALIZATION_FAILED';

UPDATE failure_event
SET normalization_status = 'PARTIALLY_NORMALIZED'
WHERE normalization_status IN ('RAW_RECEIVED', 'NORMALIZATION_PENDING');
