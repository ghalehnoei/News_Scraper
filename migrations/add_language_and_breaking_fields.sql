-- Migration to add language, is_breaking, and priority fields to news table

-- Add language column
ALTER TABLE news ADD COLUMN language VARCHAR(10);

-- Add is_breaking column
ALTER TABLE news ADD COLUMN is_breaking BOOLEAN DEFAULT FALSE NOT NULL;

-- Add priority column
ALTER TABLE news ADD COLUMN priority INTEGER;

-- Create index on language for performance
CREATE INDEX IF NOT EXISTS idx_news_language ON news(language);

-- Create index on is_breaking for performance
CREATE INDEX IF NOT EXISTS idx_news_breaking ON news(is_breaking);

-- Create index on priority for performance
CREATE INDEX IF NOT EXISTS idx_news_priority ON news(priority);
