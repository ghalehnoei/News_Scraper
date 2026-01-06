"""Run SQL migration script."""

import asyncio
import sys
from pathlib import Path

from sqlalchemy import text
from app.db.base import engine
from app.core.logging import setup_logging

logger = setup_logging()


async def run_migration(migration_file: str):
    """Run a SQL migration file."""
    migration_path = Path("migrations") / migration_file
    
    if not migration_path.exists():
        logger.error(f"Migration file not found: {migration_path}")
        return False
    
    try:
        # Read migration SQL
        sql_content = migration_path.read_text(encoding="utf-8")
        
        logger.info(f"Running migration: {migration_file}")
        
        async with engine.begin() as conn:
            # Execute migration SQL
            await conn.execute(text(sql_content))
        
        logger.info(f"Migration {migration_file} completed successfully!")
        print(f"Migration {migration_file} completed successfully!")
        return True
        
    except Exception as e:
        logger.error(f"Migration failed: {e}", exc_info=True)
        print(f"Migration failed: {e}")
        return False


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python run_migration.py <migration_file.sql>")
        print("Example: python run_migration.py add_is_international_field.sql")
        sys.exit(1)
    
    migration_file = sys.argv[1]
    success = asyncio.run(run_migration(migration_file))
    sys.exit(0 if success else 1)

