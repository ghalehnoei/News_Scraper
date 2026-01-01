"""UI routes for newsroom preview."""

import re
from typing import List, Optional
from uuid import UUID

import bleach
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse
from jinja2 import Environment
from sqlalchemy import select, desc, func, or_, and_, cast, String
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.dependencies import get_db
from app.core.config import settings
from app.core.logging import setup_logging
from app.core.category_normalizer import NORMALIZED_CATEGORIES
from app.db.models import News
from app.storage.s3 import generate_presigned_url

logger = setup_logging()

router = APIRouter(prefix="/ui", tags=["ui"])

# Persian names for sources
SOURCE_NAMES = {
    "mehrnews": "خبرگزاری مهر",
    "isna": "خبرگزاری ایسنا",
    "irna": "خبرگزاری ایرنا",
    "tasnim": "خبرگزاری تسنیم",
    "fars": "خبرگزاری فارس",
    "iribnews": "خبرگزاری صداوسیما",
    "ilna": "ایلنا",
}

# Color codes for sources
SOURCE_COLORS = {
    "mehrnews": "#e74c3c",  # قرمز
    "isna": "#3498db",      # آبی
    "irna": "#2ecc71",      # سبز
    "tasnim": "#f39c12",    # نارنجی
    "fars": "#9b59b6",      # بنفش
    "iribnews": "#16a085",  # فیروزه‌ای
    "ilna": "#e67e22",      # نارنجی تیره
}

# Persian names for normalized categories
CATEGORY_NAMES = {
    "politics": "سیاسی",
    "economy": "اقتصادی",
    "society": "اجتماعی",
    "international": "بین‌الملل",
    "culture": "فرهنگی",
    "sports": "ورزشی",
    "science": "علمی",
    "technology": "فناوری",
    "health": "سلامت",
    "provinces": "استان‌ها",
    "other": "سایر",
}

# Templates instance (will be set by main app)
templates: Environment = None


def set_templates(templates_instance: Environment):
    """Set templates instance from main app."""
    global templates
    templates = templates_instance


# Allowed HTML tags and attributes for article body
ALLOWED_TAGS = [
    "p", "br", "strong", "em", "u", "h1", "h2", "h3", "h4", "h5", "h6",
    "ul", "ol", "li", "a", "img", "blockquote", "div", "span"
]

ALLOWED_ATTRIBUTES = {
    "a": ["href", "title"],
    "img": ["src", "alt", "title", "width", "height"],
    "div": ["class"],
    "span": ["class"],
}


def sanitize_html(html: str) -> str:
    """
    Sanitize HTML content to prevent XSS attacks.

    Args:
        html: Raw HTML content

    Returns:
        Sanitized HTML
    """
    if not html:
        return ""
    return bleach.clean(
        html,
        tags=ALLOWED_TAGS,
        attributes=ALLOWED_ATTRIBUTES,
        strip=True
    )


@router.get("/news", response_class=HTMLResponse)
async def news_grid(
    request: Request,
    limit: int = 20,
    offset: int = 0,
    source: Optional[str] = None,
    category: Optional[str] = None,
    q: Optional[str] = None,
    db: AsyncSession = Depends(get_db),
):
    """
    Display latest news in grid layout with pagination support.

    Args:
        request: FastAPI request object
        limit: Maximum number of articles to display (default=20)
        offset: Number of items to skip (default=0)
        source: Optional source filter
        category: Optional category filter
        db: Database session

    Returns:
        HTML response with news grid
    """
    # Normalize search query
    search_error = None
    search_conditions = None
    if q is not None and q.strip():
        q = q.strip()
        if len(q) < 2:
            search_error = "عبارت جستجو باید حداقل ۲ کاراکتر باشد."
        else:
            # Split query into words (split by whitespace)
            words = [word.strip() for word in q.split() if word.strip()]
            
            if not words:
                search_error = "عبارت جستجو معتبر نیست."
            else:
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

    # Get list of all available normalized categories (needed for filtering)
    categories_query = select(func.distinct(News.category)).where(News.category.isnot(None)).order_by(News.category)
    categories_result = await db.execute(categories_query)
    normalized_categories = [row[0] for row in categories_result.all() if row[0]]
    
    # Get list of all available raw categories (needed for filtering)
    raw_categories_query = select(func.distinct(News.raw_category)).where(News.raw_category.isnot(None)).order_by(News.raw_category)
    raw_categories_result = await db.execute(raw_categories_query)
    raw_categories = [row[0] for row in raw_categories_result.all() if row[0]]

    # Build query - apply filters BEFORE pagination
    query = select(News).order_by(desc(News.created_at))

    if source:
        query = query.where(News.source == source)
    
    if category:
        # Check if category is a normalized category or raw category
        if category in normalized_categories:
            # Filter by normalized category
            query = query.where(News.category == category)
        elif category in raw_categories:
            # Filter by raw category
            query = query.where(News.raw_category == category)

    if search_conditions is not None:
        query = query.where(search_conditions)

    # Apply pagination AFTER filters
    query = query.offset(offset).limit(limit)

    result = await db.execute(query)
    articles = result.scalars().all()

    # Get list of all available sources
    sources_query = select(func.distinct(News.source)).order_by(News.source)
    sources_result = await db.execute(sources_query)
    available_sources = [row[0] for row in sources_result.all()]
    
    # Map sources to Persian names
    # NOTE: use a different variable name to avoid shadowing the incoming `source` param
    sources_with_names = []
    for source_key in available_sources:
        persian_name = SOURCE_NAMES.get(source_key, source_key)
        color = SOURCE_COLORS.get(source_key, "#95a5a6")  # Default gray
        sources_with_names.append({
            "key": source_key,
            "name": persian_name,
            "color": color
        })
    
    # Map normalized categories to Persian names for display
    # Only show categories that are in our normalized category set
    available_categories = []
    for cat in normalized_categories:
        # Only include if it's a valid normalized category (in our enum set)
        if cat in NORMALIZED_CATEGORIES:
            persian_name = CATEGORY_NAMES.get(cat, cat)
            available_categories.append({
                "key": cat,
                "name": persian_name,
                "type": "normalized",
            })

    articles_data = []
    for article in articles:
        # Generate presigned URL for image if it exists
        image_url = article.image_url
        if image_url:
            try:
                # Extract S3 key from URL
                # Stored format can be:
                # 1. s3://bucket/path (e.g., s3://output/news-images/iribnews/2025/12/30/abc123.jpg)
                # 2. {endpoint}/{bucket}/{key} (e.g., https://gpmedia.iribnews.ir/output/news-images/...)
                # 3. Just the key (e.g., news-images/iribnews/2025/12/30/abc123.jpg)
                if image_url.startswith("s3://"):
                    # Parse s3://bucket/path format
                    # Remove "s3://" prefix
                    path_after_s3 = image_url[5:]  # Remove "s3://"
                    # Split bucket and key
                    parts = path_after_s3.split("/", 1)
                    if len(parts) == 2:
                        path_bucket, s3_key = parts
                        # Use the path after bucket as key (regardless of bucket name)
                        s3_key = s3_key
                    else:
                        # No path after bucket, this shouldn't happen but use as-is
                        s3_key = path_after_s3
                    logger.debug(f"Extracted S3 key from s3:// URL: {s3_key}")
                elif image_url.startswith(settings.s3_endpoint):
                    # Remove endpoint and bucket to get the key
                    prefix = f"{settings.s3_endpoint}/{settings.s3_bucket}/"
                    if image_url.startswith(prefix):
                        s3_key = image_url[len(prefix):]
                    else:
                        # Try without trailing slash
                        prefix = f"{settings.s3_endpoint}/{settings.s3_bucket}"
                        if image_url.startswith(prefix):
                            s3_key = image_url[len(prefix):].lstrip("/")
                        else:
                            s3_key = image_url
                else:
                    # Assume it's already an S3 key
                    s3_key = image_url
                
                presigned = await generate_presigned_url(s3_key)
                if presigned:
                    image_url = presigned
            except Exception as e:
                # If presigned URL generation fails, use original URL
                logger.warning(f"Failed to generate presigned URL for {image_url}: {e}")

        # Format dates for display
        published_at = article.published_at
        if published_at:
            # Keep original for Persian conversion in template
            pass
        
        # Category is already normalized, no need to extract main/sub
        # Use raw_category for display if needed, but normalized category for filtering
        
        # Get Persian name and color for source
        source_persian = SOURCE_NAMES.get(article.source, article.source)
        source_color = SOURCE_COLORS.get(article.source, "#95a5a6")
        
        articles_data.append({
            "id": str(article.id),
            "title": article.title,
            "summary": article.summary,
            "source": article.source,
            "source_persian": source_persian,
            "source_color": source_color,
            "published_at": published_at,
            "image_url": image_url,
            "category": article.category,  # Normalized category
            "category_persian": CATEGORY_NAMES.get(article.category, article.category) if article.category else None,  # Persian name for display
            "raw_category": article.raw_category,  # Original category for reference
        })

    # Calculate pagination info
    count_query = select(func.count(News.id))
    if source:
        count_query = count_query.where(News.source == source)
    if category:
        # Check if category is a normalized category or raw category
        if category in normalized_categories:
            # Filter by normalized category
            count_query = count_query.where(News.category == category)
        elif category in raw_categories:
            # Filter by raw category
            count_query = count_query.where(News.raw_category == category)
    if search_conditions is not None:
        count_query = count_query.where(search_conditions)
    
    total_result = await db.execute(count_query)
    total = total_result.scalar_one()
    has_more = (offset + limit) < total

    # Extract search words for highlighting
    search_words = []
    if q is not None and q.strip() and len(q.strip()) >= 2:
        search_words = [word.strip() for word in q.strip().split() if word.strip()]
    
    template = templates.get_template("news_grid.html")
    
    # Normalize source for comparison (ensure it's a string or None)
    # Also handle empty strings as None
    current_source_normalized = None
    if source and source.strip():
        current_source_normalized = source.strip()
    
    # Debug logging
    logger.debug(f"news_grid: source={source}, current_source_normalized={current_source_normalized}, category={category}")
    
    html_content = template.render(
        request=request,
        articles=articles_data,
        sources=sources_with_names,
        current_source=current_source_normalized,
        categories=available_categories,
        current_category=category,
        current_query=q,
        search_words=search_words,
        search_error=search_error,
        pagination={
            "limit": limit,
            "offset": offset,
            "total": total,
            "has_more": has_more,
        },
    )
    return HTMLResponse(content=html_content)


@router.get("/news/{news_id}", response_class=HTMLResponse)
async def news_detail(
    news_id: str,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """
    Display full article details.

    Args:
        news_id: News article UUID
        request: FastAPI request object
        db: Database session

    Returns:
        HTML response with article details

    Raises:
        HTTPException: 404 if article not found
    """
    try:
        # Validate UUID format
        UUID(news_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid news ID format")

    # Handle PostgreSQL UUID vs SQLite string
    # PostgreSQL stores UUID as UUID type, SQLite as string
    from app.core.config import settings
    if "postgresql" in settings.database_url.lower():
        # For PostgreSQL, cast UUID column to text for comparison with string
        result = await db.execute(
            select(News).where(cast(News.id, String) == news_id)
        )
    else:
        # For SQLite, compare as strings
        result = await db.execute(select(News).where(News.id == news_id))
    article = result.scalar_one_or_none()

    if not article:
        raise HTTPException(status_code=404, detail="News article not found")

    # Replace S3 paths in body_html with presigned URLs BEFORE sanitization
    # This ensures we catch all s3:// URLs before any HTML processing
    raw_body_html = article.body_html or ""
    
    if raw_body_html:
        # Find all S3 paths in raw body_html (format: s3://bucket/path)
        s3_pattern = r's3://([^/]+)/([^"\'>\s\)]+)'
        s3_matches = re.findall(s3_pattern, raw_body_html)
        
        if s3_matches:
            logger.debug(f"Found {len(s3_matches)} S3 URLs in raw body_html to replace")
            s3_replacements = {}
            
            for bucket, s3_path_raw in set(s3_matches):
                # The s3_path is already the S3 key (path after bucket name)
                s3_key = s3_path_raw
                
                try:
                    presigned = await generate_presigned_url(s3_key)
                    if presigned:
                        s3_full_path = f"s3://{bucket}/{s3_path_raw}"
                        s3_replacements[s3_full_path] = presigned
                        logger.debug(f"Generated presigned URL for s3://{bucket}/{s3_path_raw[:50]}... -> {presigned[:60]}...")
                except Exception as e:
                    logger.warning(f"Error generating presigned URL for s3://{bucket}/{s3_path_raw}: {e}", exc_info=True)
            
            # Replace all s3:// URLs with presigned URLs
            for s3_path, presigned_url in s3_replacements.items():
                raw_body_html = raw_body_html.replace(s3_path, presigned_url)
            
            if s3_replacements:
                logger.info(f"Replaced {len(s3_replacements)} S3 paths with presigned URLs in raw body_html")
    
    # Now sanitize the HTML body (after S3 URL replacement)
    sanitized_body = sanitize_html(raw_body_html) if raw_body_html else ""
    
    # Remove any remaining Fars CDN images from body_html
    # These are original source images that should be removed since we have S3 versions
    if sanitized_body:
        try:
            from selectolax.parser import HTMLParser
            body_tree = HTMLParser(sanitized_body)
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
                        logger.debug(f"Removing Fars CDN image from display: {src[:60]}...")
            
            # Remove the images and their empty containers
            for img in images_to_remove:
                # Try to remove the parent element if it's a link or container
                parent = img.parent
                if parent:
                    # If parent is an <a> tag, remove the entire link
                    if parent.tag == "a":
                        # Check if parent's parent is also an empty container
                        grandparent = parent.parent
                        if grandparent and grandparent.tag == "div":
                            # Check if grandparent only contains this link
                            has_other_content = False
                            for sibling in grandparent.iter():
                                if sibling != grandparent and sibling != parent and sibling != img:
                                    sibling_text = sibling.text(strip=True) if sibling.text() else ""
                                    if sibling_text or (sibling.tag not in ['script', 'style', 'comment']):
                                        has_other_content = True
                                        break
                            if not has_other_content:
                                grandparent.decompose()
                            else:
                                parent.decompose()
                        else:
                            parent.decompose()
                    elif parent.tag == "div":
                        # Check if div only contains this image
                        has_other_content = False
                        for child in parent.iter():
                            if child != parent and child != img:
                                child_text = child.text(strip=True) if child.text() else ""
                                if child_text or (child.tag not in ['script', 'style', 'comment']):
                                    has_other_content = True
                                    break
                        if not has_other_content:
                            parent.decompose()
                        else:
                            img.decompose()
                    else:
                        # Otherwise, just remove the img tag
                        img.decompose()
                else:
                    img.decompose()
            
            if images_to_remove:
                sanitized_body = body_tree.html
                logger.info(f"Removed {len(images_to_remove)} Fars CDN images from body_html for display")
            
            # Clean up empty containers and ensure proper spacing
            try:
                body_tree_clean = HTMLParser(sanitized_body)
                
                # Remove empty divs and containers
                empty_divs = []
                for div in body_tree_clean.css("div"):
                    # Check if div is empty or only contains whitespace/comment nodes
                    inner_text = div.text(strip=True) if div.text() else ""
                    inner_html = div.html.strip() if div.html else ""
                    # Remove HTML comments and check if truly empty
                    inner_html_clean = re.sub(r'<!--.*?-->', '', inner_html, flags=re.DOTALL).strip()
                    
                    if not inner_text and (not inner_html_clean or inner_html_clean in ['<div></div>', '<div> </div>']):
                        # Check if it has any meaningful children
                        has_children = False
                        for child in div.iter():
                            if child != div and child.tag not in ['script', 'style', 'comment']:
                                child_text = child.text(strip=True) if child.text() else ""
                                if child_text or (child.tag == 'img'):
                                    has_children = True
                                    break
                        
                        if not has_children:
                            empty_divs.append(div)
                
                for div in empty_divs:
                    div.decompose()
                
                if empty_divs:
                    sanitized_body = body_tree_clean.html
                    logger.debug(f"Removed {len(empty_divs)} empty containers from body_html")
            except Exception as e:
                logger.warning(f"Error cleaning empty containers: {e}", exc_info=True)
        except Exception as e:
            logger.warning(f"Error removing Fars CDN images: {e}", exc_info=True)
    
    # Final safety check: replace any remaining s3:// URLs in sanitized body
    # (in case sanitization somehow reintroduced them or we missed some)
    if sanitized_body:
        remaining_s3_pattern = r's3://([^/]+)/([^"\'>\s\)]+)'
        remaining_s3_matches = re.findall(remaining_s3_pattern, sanitized_body)
        if remaining_s3_matches:
            logger.warning(f"Found {len(set(remaining_s3_matches))} remaining S3 URLs in sanitized body. Fixing...")
            for bucket, s3_path_raw in set(remaining_s3_matches):
                s3_key = s3_path_raw
                try:
                    presigned = await generate_presigned_url(s3_key)
                    if presigned:
                        s3_full_path = f"s3://{bucket}/{s3_path_raw}"
                        sanitized_body = sanitized_body.replace(s3_full_path, presigned)
                        logger.debug(f"Fixed remaining S3 URL in sanitized body: {s3_full_path[:60]}... -> {presigned[:60]}...")
                except Exception as e:
                    logger.warning(f"Failed to fix remaining S3 URL s3://{bucket}/{s3_path_raw}: {e}", exc_info=True)

    # Generate presigned URL for image if it exists
    image_url = article.image_url
    if image_url:
        try:
            # Extract S3 key from URL
            # Stored format: {endpoint}/{bucket}/{key}
            if image_url.startswith(settings.s3_endpoint):
                # Remove endpoint and bucket to get the key
                prefix = f"{settings.s3_endpoint}/{settings.s3_bucket}/"
                if image_url.startswith(prefix):
                    s3_key = image_url[len(prefix):]
                else:
                    # Try without trailing slash
                    prefix = f"{settings.s3_endpoint}/{settings.s3_bucket}"
                    if image_url.startswith(prefix):
                        s3_key = image_url[len(prefix):].lstrip("/")
                    else:
                        s3_key = image_url
            else:
                # Assume it's already an S3 key
                s3_key = image_url
            
            presigned = await generate_presigned_url(s3_key)
            if presigned:
                image_url = presigned
        except Exception as e:
            # If presigned URL generation fails, use original URL
            logger.warning(f"Failed to generate presigned URL for {image_url}: {e}")

    article_data = {
        "id": str(article.id),
        "title": article.title,
        "source": article.source,
        "body_html": sanitized_body,
        "summary": article.summary,
        "published_at": article.published_at,
        "image_url": image_url,
        "category": article.category,
        "url": article.url,
    }

    template = templates.get_template("news_detail.html")
    html_content = template.render(
        request=request,
        article=article_data,
    )
    return HTMLResponse(content=html_content)

