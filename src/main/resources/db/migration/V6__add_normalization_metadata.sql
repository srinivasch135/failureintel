ALTER TABLE failure_event
ADD COLUMN IF NOT EXISTS normalization_metadata JSONB;
