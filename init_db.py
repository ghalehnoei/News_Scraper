"""Initialize database tables for local development."""

import asyncio
from app.db.base import Base, engine
from app.core.logging import setup_logging

logger = setup_logging()


async def init_db():
    """Create all database tables."""
    try:
        async with engine.begin() as conn:
            # Create all tables
            await conn.run_sync(Base.metadata.create_all)
            logger.info("Database tables created successfully")
            print("✅ Database initialized successfully!")
    except Exception as e:
        logger.error(f"Database initialization failed: {e}", exc_info=True)
        print(f"❌ Database initialization failed: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(init_db())

