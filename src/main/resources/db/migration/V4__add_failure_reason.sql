ALTER TABLE failure_event
ADD COLUMN IF NOT EXISTS failure_reason TEXT;
