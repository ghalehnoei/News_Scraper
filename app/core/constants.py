"""
Application constants.

This module contains all application-wide constants including:
- Source definitions (international sources, Reuters sources)
- Default values (language, priority, source type)
- Search and pagination constraints
- Breaking news keywords
"""

# International news sources
INTERNATIONAL_SOURCES = {
    "reuters_photos",
    "reuters_text",
    "reuters_video",
    "afp_text",
    "afp_video",
    "afp_photo",
    "aptn_text",
    "aptn_video",
    "aptn_photo",
}

# Reuters sources (LTR text direction)
REUTERS_SOURCES = ["reuters_photos", "reuters_text", "reuters_video"]

# Breaking news keywords
BREAKING_NEWS_KEYWORDS = ["BREAKING", "URGENT", "FLASH"]

# Default values
DEFAULT_LANGUAGE = "en"
DEFAULT_PRIORITY = 3
DEFAULT_SOURCE_TYPE = "internal"

# Search constraints
MIN_SEARCH_QUERY_LENGTH = 2

# Pagination defaults
DEFAULT_PAGE_LIMIT = 20
MAX_PAGE_LIMIT = 100
DEFAULT_PAGE_OFFSET = 0

