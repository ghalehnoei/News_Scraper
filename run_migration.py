"""Run database migration to add raw_category column."""

import asyncio
from sqlalchemy import text
from app.db.base import AsyncSessionLocal
from app.core.logging import setup_logging
from app.core.config import settings

logger = setup_logging()


async def run_migration():
    """Add raw_category column to news table."""
    async with AsyncSessionLocal() as db:
        try:
            is_sqlite = "sqlite" in settings.database_url

            if is_sqlite:
                # SQLite: Check if column exists using pragma
                result = await db.execute(text("""
                    SELECT name FROM pragma_table_info('news') WHERE name='raw_category'
                """))
                column_exists = result.fetchone() is not None
                alter_sql = "ALTER TABLE news ADD COLUMN raw_category VARCHAR(200);"
            else:
                # PostgreSQL: Use IF NOT EXISTS
                result = await db.execute(text("""
                    SELECT column_name FROM information_schema.columns
                    WHERE table_name='news' AND column_name='raw_category'
                """))
                column_exists = result.fetchone() is not None
                alter_sql = "ALTER TABLE news ADD COLUMN IF NOT EXISTS raw_category VARCHAR(200);"

            if not column_exists:
                await db.execute(text(alter_sql))
                logger.info("Migration: Added raw_category column")
            else:
                logger.info("Migration: raw_category column already exists")

            await db.commit()
            logger.info("Migration completed successfully")
            print("✅ Migration completed successfully!")

        except Exception as e:
            await db.rollback()
            logger.error(f"Migration failed: {e}", exc_info=True)
            print(f"❌ Migration failed: {e}")
            raise


if __name__ == "__main__":
    asyncio.run(run_migration())

