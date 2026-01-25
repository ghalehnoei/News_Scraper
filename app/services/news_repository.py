"""News repository for database operations."""

from typing import Optional
from datetime import datetime
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import News
from app.db.session import AsyncSessionLocal
from app.core.category_normalizer import normalize_category


class NewsRepository:
    """
    Repository for News model operations.
    
    Provides centralized database operations for news articles.
    """

    @staticmethod
    async def get_by_url(url: str) -> Optional[News]:
        """
        Get news article by URL.
        
        Args:
            url: Article URL
            
        Returns:
            News object if found, None otherwise
        """
        try:
            async with AsyncSessionLocal() as db:
                result = await db.execute(
                    select(News).where(News.url == url)
                )
                return result.scalar_one_or_none()
        except Exception as e:
            print(f"Error getting news by URL: {e}")
            return None

    @staticmethod
    async def save(news: News) -> bool:
        """
        Save news article to database.
        
        Args:
            news: News object to save
            
        Returns:
            True if saved successfully, False otherwise
        """
        try:
            async with AsyncSessionLocal() as db:
                db.add(news)
                await db.commit()
                return True
        except Exception as e:
            await db.rollback()
            print(f"Error saving news: {e}")
            return False

    @staticmethod
    async def update(news: News) -> bool:
        """
        Update existing news article.
        
        Args:
            news: News object with updated values
            
        Returns:
            True if updated successfully, False otherwise
        """
        try:
            async with AsyncSessionLocal() as db:
                await db.merge(news)
                await db.commit()
                return True
        except Exception as e:
            await db.rollback()
            print(f"Error updating news: {e}")
            return False

    @staticmethod
    async def get_recent(limit: int = 50) -> list[News]:
        """
        Get recent news articles.
        
        Args:
            limit: Maximum number of articles to return
            
        Returns:
            List of News objects
        """
        try:
            async with AsyncSessionLocal() as db:
                result = await db.execute(
                    select(News).order_by(News.created_at.desc()).limit(limit)
                )
                return result.scalars().all()
        except Exception as e:
            print(f"Error getting recent news: {e}")
            return []

    @staticmethod
    async def create_news_object(
        source: str,
        title: str,
        body_html: str,
        url: str,
        published_at: str,
        summary: str = "",
        image_url: str = "",
        raw_category: str = "",
        language: str = "en",
        priority: int = 3,
        is_international: bool = False,
        source_type: str = "internal"
    ) -> News:
        """
        Create a News object with proper normalization.
        
        Args:
            source: Source name
            title: Article title
            body_html: HTML body content
            url: Article URL
            published_at: Publication date string
            summary: Article summary
            image_url: Image URL
            raw_category: Raw category from source
            language: Language code
            priority: Priority level
            is_international: Whether international news
            source_type: Source type (internal/external)
            
        Returns:
            Configured News object
        """
        # Normalize category
        normalized_category, raw_cat = normalize_category(
            source,
            raw_category
        )
        
        return News(
            source=source,
            title=title,
            body_html=body_html,
            summary=summary,
            url=url,
            published_at=published_at,
            image_url=image_url,
            category=normalized_category,
            raw_category=raw_cat,
            language=language,
            priority=priority,
            is_international=is_international,
            source_type=source_type,
        )
