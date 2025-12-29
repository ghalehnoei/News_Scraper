"""News endpoints."""

import re
import logging
from typing import List, Optional
from uuid import UUID

from fastapi import APIRouter, Query, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import select, desc, func, or_, and_, or_
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.dependencies import get_db
from app.core.config import settings
from app.core.date_utils import format_persian_date
from app.db.models import News
from app.storage.s3 import generate_presigned_url

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/news", tags=["news"])

# Persian names for sources
SOURCE_NAMES = {
    "mehrnews": "خبرگزاری مهر",
    "isna": "خبرگزاری ایسنا",
    "irna": "خبرگزاری ایرنا",
    "tasnim": "خبرگزاری تسنیم",
    "fars": "خبرگزاری فارس",
}


class PaginationInfo(BaseModel):
    """Pagination metadata."""
    limit: int
    offset: int
    total: int
    has_more: bool


class PaginatedNewsResponse(BaseModel):
    """Paginated news response."""
    items: List[dict]
    pagination: PaginationInfo


@router.get("/latest", response_model=PaginatedNewsResponse)
async def get_latest_news(
    source: Optional[str] = Query(None, description="Filter by source name"),
    q: Optional[str] = Query(None, description="Search keyword"),
    limit: int = Query(20, ge=1, le=100, description="Maximum number of results"),
    offset: int = Query(0, ge=0, description="Number of items to skip"),
    db: AsyncSession = Depends(get_db),
) -> PaginatedNewsResponse:
    """
    Get latest news articles with pagination support.

    Args:
        source: Optional source name filter
        limit: Maximum number of results (1-100, default=20)
        offset: Number of items to skip (default=0)
        db: Database session

    Returns:
        Paginated news response with items and pagination metadata
    """
    # Normalize and validate search query
    search_conditions = None
    if q is not None:
        q = q.strip()
        if q == "":
            q = None
        elif len(q) < 2:
            raise HTTPException(status_code=400, detail="Search query must be at least 2 characters")
        else:
            # Split query into words (split by whitespace)
            words = [word.strip() for word in q.split() if word.strip()]
            
            if words:
                # Build conditions: each word must appear in title, summary, or body_html
                # All words must be present (AND logic between words)
                word_conditions = []
                for word in words:
                    # Escape wildcard characters for ILIKE
                    escaped_word = word.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
                    word_pattern = f"%{escaped_word}%"
                    
                    # Word must appear in at least one field (OR within word)
                    word_condition = or_(
                        News.title.ilike(word_pattern, escape="\\"),
                        News.summary.ilike(word_pattern, escape="\\"),
                        News.body_html.ilike(word_pattern, escape="\\"),
                    )
                    word_conditions.append(word_condition)
                
                # All words must be present (AND between words)
                if word_conditions:
                    search_conditions = and_(*word_conditions)

    # Build base query for counting
    count_query = select(func.count(News.id))
    
    # Build query for fetching items
    query = select(News).order_by(desc(News.created_at))

    # Apply source filter if provided
    if source:
        query = query.where(News.source == source)
        count_query = count_query.where(News.source == source)

    # Apply text search if provided
    if search_conditions is not None:
        query = query.where(search_conditions)
        count_query = count_query.where(search_conditions)

    # Get total count
    total_result = await db.execute(count_query)
    total = total_result.scalar_one()

    # Apply pagination
    query = query.offset(offset).limit(limit)

    # Execute query
    result = await db.execute(query)
    articles = result.scalars().all()

    # Build response items
    items = [
        {
            "id": str(article.id),
            "source": article.source,
            "title": article.title,
            "summary": article.summary,
            "url": article.url,
            "published_at": article.published_at,
            "created_at": article.created_at.isoformat() if article.created_at else None,
            "image_url": article.image_url,
            "category": article.category,
        }
        for article in articles
    ]

    # Calculate has_more
    has_more = (offset + limit) < total

    return PaginatedNewsResponse(
        items=items,
        pagination=PaginationInfo(
            limit=limit,
            offset=offset,
            total=total,
            has_more=has_more,
        ),
    )


@router.get("/{news_id}")
async def get_news_by_id(
    news_id: str,
    db: AsyncSession = Depends(get_db),
) -> dict:
    """
    Get news article by ID.
    
    Args:
        news_id: News article UUID
        db: Database session
        
    Returns:
        News article data
        
    Raises:
        HTTPException: 404 if article not found
    """
    try:
        article_uuid = UUID(news_id)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Invalid news ID format: {news_id}")

    result = await db.execute(select(News).where(News.id == article_uuid))
    article = result.scalar_one_or_none()

    if not article:
        raise HTTPException(status_code=404, detail=f"News article not found: {news_id}")

    # Generate presigned URL for image if it exists
    image_url = article.image_url
    if image_url:
        try:
            # Extract S3 key from URL
            if image_url.startswith(settings.s3_endpoint):
                prefix = f"{settings.s3_endpoint}/{settings.s3_bucket}/"
                if image_url.startswith(prefix):
                    s3_key = image_url[len(prefix):]
                else:
                    prefix = f"{settings.s3_endpoint}/{settings.s3_bucket}"
                    if image_url.startswith(prefix):
                        s3_key = image_url[len(prefix):].lstrip("/")
                    else:
                        s3_key = image_url
            else:
                s3_key = image_url
            
            presigned = await generate_presigned_url(s3_key)
            if presigned:
                image_url = presigned
        except Exception:
            # If presigned URL generation fails, use original URL
            pass

    # Convert published_at to Persian date
    published_at_persian = article.published_at
    if article.published_at:
        try:
            published_at_persian = format_persian_date(article.published_at)
        except Exception:
            # If conversion fails, use original
            pass

    # Get Persian name for source
    source_persian = SOURCE_NAMES.get(article.source, article.source)
    
    # Replace S3 paths in body_html with presigned URLs
    body_html = article.body_html or ""
    if body_html:
        # Find all S3 paths in body_html (format: s3://bucket/path)
        s3_pattern = r's3://([^/]+)/([^"\'>\s\)]+)'
        s3_matches = re.findall(s3_pattern, body_html)
        
        if s3_matches:
            s3_replacements = {}
            for bucket, s3_path_raw in set(s3_matches):
                # The s3_path is already the S3 key (path after bucket name)
                s3_key = s3_path_raw
                
                try:
                    presigned = await generate_presigned_url(s3_key)
                    if presigned:
                        s3_full_path = f"s3://{bucket}/{s3_path_raw}"
                        s3_replacements[s3_full_path] = presigned
                except Exception as e:
                    logger.warning(f"Error generating presigned URL for s3://{bucket}/{s3_path_raw}: {e}", exc_info=True)
            
            # Replace all s3:// URLs with presigned URLs
            for s3_path, presigned_url in s3_replacements.items():
                body_html = body_html.replace(s3_path, presigned_url)
            
            if s3_replacements:
                logger.debug(f"Replaced {len(s3_replacements)} S3 paths with presigned URLs in API response")
    
    # Remove any remaining Fars CDN images from body_html
    # These are original source images that should be removed since we have S3 versions
    if body_html:
        try:
            from selectolax.parser import HTMLParser
            body_tree = HTMLParser(body_html)
            fars_cdn_patterns = ["cdn.farsnews.ir", "farsnews.ir"]
            
            images_to_remove = []
            for img in body_tree.css("img"):
                if not img.attributes:
                    continue
                
                # Check all possible src attributes
                src = (
                    img.attributes.get("src") or 
                    img.attributes.get("data-src") or 
                    img.attributes.get("data-lazy-src") or 
                    img.attributes.get("data-original") or ""
                )
                
                if src and any(pattern in src for pattern in fars_cdn_patterns):
                    # Skip if it's already an S3 URL or presigned URL
                    if not src.startswith("s3://") and settings.s3_endpoint not in src:
                        images_to_remove.append(img)
                        logger.debug(f"Removing Fars CDN image from API response: {src[:60]}...")
            
            # Remove the images
            for img in images_to_remove:
                # Try to remove the parent element if it's a link or container
                parent = img.parent
                if parent:
                    # If parent is an <a> tag, remove the entire link
                    if parent.tag == "a":
                        parent.decompose()
                    else:
                        # Otherwise, just remove the img tag
                        img.decompose()
                else:
                    img.decompose()
            
            if images_to_remove:
                body_html = body_tree.html
                logger.info(f"Removed {len(images_to_remove)} Fars CDN images from body_html in API response")
        except Exception as e:
            logger.warning(f"Error removing Fars CDN images in API: {e}", exc_info=True)
    
    return {
        "id": str(article.id),
        "source": article.source,
        "source_persian": source_persian,
        "title": article.title,
        "body_html": body_html,
        "summary": article.summary,
        "url": article.url,
        "published_at": published_at_persian,
        "created_at": article.created_at.isoformat() if article.created_at else None,
        "image_url": image_url,
        "category": article.category,
        "raw_category": article.raw_category,  # Original category for display
    }

