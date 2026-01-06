-- Migration to add a new field 'source_type' to identify news sources as internal or external

ALTER TABLE news_sources ADD COLUMN source_type TEXT;

-- Update existing records: set 'internal' for all except Reuters, which should be 'external'
UPDATE news_sources SET source_type = 'internal';
UPDATE news_sources SET source_type = 'external' WHERE name = 'Reuters';