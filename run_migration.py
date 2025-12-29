"""Run database migration to add raw_category column."""

import asyncio
from sqlalchemy import text
from app.db.base import AsyncSessionLocal
from app.core.logging import setup_logging

logger = setup_logging()


async def run_migration():
    """Add raw_category column to news table."""
    async with AsyncSessionLocal() as db:
        try:
            # Add raw_category column
            await db.execute(text("""
                ALTER TABLE news ADD COLUMN IF NOT EXISTS raw_category VARCHAR(200);
            """))
            
            # Add comments
            await db.execute(text("""
                COMMENT ON COLUMN news.raw_category IS 'Original category string from news source (before normalization)';
            """))
            
            await db.execute(text("""
                COMMENT ON COLUMN news.category IS 'Normalized category (politics, economy, society, etc.)';
            """))
            
            await db.commit()
            logger.info("Migration completed successfully: raw_category column added")
            print("✅ Migration completed successfully!")
            
        except Exception as e:
            await db.rollback()
            logger.error(f"Migration failed: {e}", exc_info=True)
            print(f"❌ Migration failed: {e}")
            raise


if __name__ == "__main__":
    asyncio.run(run_migration())

