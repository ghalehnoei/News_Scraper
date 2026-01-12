#!/usr/bin/env python3

import asyncio
from app.db.session import get_db
from sqlalchemy import text

async def check_afp_articles():
    async for db in get_db():
        # Get latest AFP articles
        result = await db.execute(
            text("SELECT title, language, priority FROM news WHERE source = 'afp_text' ORDER BY created_at DESC LIMIT 5")
        )
        rows = result.fetchall()

        print('Latest AFP articles:')
        for row in rows:
            print(f'Title: {row[0][:50]}...')
            print(f'Language: {row[1]}')
            print(f'Priority: {row[2]}')
            print('---')

if __name__ == "__main__":
    asyncio.run(check_afp_articles())
