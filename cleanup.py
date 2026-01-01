import asyncio
from sqlalchemy import delete
from app.db.base import AsyncSessionLocal
from app.db.models import News

async def main():
    async with AsyncSessionLocal() as db:
        await db.execute(delete(News).where(News.source == "ilna"))
        await db.commit()
        print("Done")

asyncio.run(main())
