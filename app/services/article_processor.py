"""Article processor for news articles."""

from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime

from app.core.category_normalizer import normalize_category


class ArticleProcessor:
    """
    Processor for news articles.
    
    Provides common article processing functionality including:
    - Category normalization (mapping source-specific categories to standard categories)
    - HTML body generation (creating formatted HTML from article content)
    - Date parsing (converting various date formats to ISO format)
    - News object creation (building normalized news dictionaries)
    
    This class is designed to be instantiated per source, allowing for
    source-specific processing while sharing common functionality.
    
    Attributes:
        source_name: Name of the news source this processor handles
    """

    def __init__(self, source_name: str):
        """
        Initialize article processor.
        
        Args:
            source_name: Name of the news source
        """
        self.source_name = source_name

    def normalize_category(self, raw_category: str) -> Tuple[str, str]:
        """
        Normalize raw category to standardized category.
        
        Args:
            raw_category: Raw category string from source
            
        Returns:
            Tuple of (normalized_category, raw_category)
        """
        return normalize_category(self.source_name, raw_category)

    def build_body_html(
        self,
        title: str,
        summary: str,
        body: str,
        body_paragraphs: Optional[List[str]] = None,
        priority: int = 3
    ) -> str:
        """
        Build HTML body for news article.
        
        Args:
            title: Article title
            summary: Article summary
            body: Article body text
            body_paragraphs: List of paragraphs (if available)
            priority: Article priority (1-5)
            
        Returns:
            HTML string for article body
        """
        body_html = ""
        
        # Add title with color based on priority
        if title:
            if priority in [1, 2]:
                body_html += f'<h2 style="color: red;">{title}</h2>'
            else:
                body_html += f'<h2>{title}</h2>'
        
        # Add summary
        if summary:
            body_html += f"<p><strong>{summary}</strong></p>"
        
        # Add body content
        if body_paragraphs:
            for paragraph in body_paragraphs:
                if paragraph.strip():
                    body_html += f'<p style="color: black;">{paragraph}</p>'
        elif body:
            # Check if body looks like HTML or plain text
            if body.strip() and len(body.strip()) > 10:
                # If it looks like it might be HTML, wrap it
                if '<' not in body:
                    body_html += f'<p style="color: black;">{body}</p>'
                else:
                    # It's already HTML, just add it
                    body_html += body
        
        # Ensure we have some content
        if not body_html or len(body_html.strip()) < 20:
            # Fallback: create a minimal body
            if title:
                body_html = f'<h2>{title}</h2><p>No content available</p>'
            else:
                body_html = '<p>No content available</p>'
        
        return body_html

    def parse_date(self, date_str: Optional[str]) -> str:
        """
        Parse date string and return ISO format.
        
        Args:
            date_str: Date string in various formats
            
        Returns:
            ISO format date string
        """
        if not date_str:
            return datetime.utcnow().isoformat()
        
        try:
            # Try to parse ISO format
            if isinstance(date_str, (int, float)):
                return datetime.fromtimestamp(date_str).isoformat()
            else:
                # Try ISO format
                return datetime.fromisoformat(date_str.replace("Z", "+00:00")).isoformat()
        except Exception:
            # Fallback to current time
            return datetime.utcnow().isoformat()

    def create_news_object(
        self,
        article_data: Dict[str, Any],
        source: str,
        url: str,
        is_international: bool = False,
        source_type: str = "external"
    ) -> Dict[str, Any]:
        """
        Create news object from article data.
        
        Args:
            article_data: Dictionary with article fields
            source: Source name
            url: Article URL
            is_international: Whether international news
            source_type: Source type
            
        Returns:
            Dictionary with normalized article data
        """
        # Normalize category
        normalized_category, raw_category = self.normalize_category(
            article_data.get("category", "")
        )
        
        # Parse published date
        published_at = self.parse_date(article_data.get("published_at", ""))
        
        # Build body HTML
        body_html = self.build_body_html(
            title=article_data.get("title", ""),
            summary=article_data.get("summary", ""),
            body=article_data.get("body", ""),
            body_paragraphs=article_data.get("body_paragraphs", []),
            priority=article_data.get("priority", 3)
        )
        
        return {
            "source": source,
            "title": article_data.get("title", ""),
            "body_html": body_html,
            "summary": article_data.get("summary", ""),
            "url": url,
            "published_at": published_at,
            "image_url": article_data.get("image_url", ""),
            "category": normalized_category,
            "raw_category": raw_category,
            "language": article_data.get("language", "en"),
            "priority": article_data.get("priority", 3),
            "is_international": is_international,
            "source_type": source_type,
        }
