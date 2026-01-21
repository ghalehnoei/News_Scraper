import asyncio
import os
from dotenv import load_dotenv

# Load environment variables BEFORE importing app modules
load_dotenv()
os.environ["USE_ENV_FILE"] = "true"

from sqlalchemy import delete
from app.db.base import AsyncSessionLocal
from app.db.models import News

async def main():
    async with AsyncSessionLocal() as db:
        result = await db.execute(delete(News).where(News.source == "aptn_text"))
        await db.commit()
        print(f"Deleted {result.rowcount} news items from IPNA")

asyncio.run(main())
