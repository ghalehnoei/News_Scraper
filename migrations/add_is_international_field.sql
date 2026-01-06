-- Migration to add is_international field to news table

-- Add is_international column
ALTER TABLE news ADD COLUMN is_international BOOLEAN DEFAULT FALSE NOT NULL;

-- Create index on is_international for performance
CREATE INDEX IF NOT EXISTS idx_news_is_international ON news(is_international);




