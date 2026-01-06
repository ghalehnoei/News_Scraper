-- Migration to add a new field 'source_type' to news table
ALTER TABLE news ADD COLUMN source_type TEXT DEFAULT 'internal' NOT NULL;

-- Create index for faster filtering
CREATE INDEX IF NOT EXISTS idx_news_source_type ON news(source_type);

-- Backfill based on existing `is_international` where present
UPDATE news SET source_type = 'external' WHERE is_international = TRUE;
