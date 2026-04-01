ALTER TABLE stream_batch_comparison
  ADD COLUMN IF NOT EXISTS root_cause_tags TEXT[] DEFAULT ARRAY[]::text[];

ALTER TABLE stream_batch_comparison
  ADD COLUMN IF NOT EXISTS details_json JSONB DEFAULT '{}'::jsonb;

CREATE INDEX IF NOT EXISTS idx_cmp_tags_gin
  ON stream_batch_comparison USING GIN (root_cause_tags);
