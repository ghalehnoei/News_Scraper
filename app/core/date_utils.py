"""Date utility functions for Persian date conversion."""

import logging

logger = logging.getLogger(__name__)


def format_persian_date(date_str):
    """Convert date string to Persian (Jalali) format in Tehran timezone."""
    if not date_str:
        return ""
    
    # If already a Persian date format (contains /), return as is
    if isinstance(date_str, str) and ('/' in date_str and len(date_str.split('/')) == 3):
        # Check if it's already Persian format (e.g., "1404/09/06")
        parts = date_str.split('/')
        if len(parts) == 3 and parts[0].isdigit() and int(parts[0]) > 1300:
            return date_str  # Already Persian date
    
    try:
        import jdatetime
        from datetime import datetime
        from dateutil import parser
        from dateutil.tz import UTC, gettz
        
        # Try to parse the date string
        date_obj = None
        
        # First try with dateutil parser (handles most formats including timezone)
        try:
            date_obj = parser.parse(date_str)
        except Exception:
            pass
        
        # If dateutil failed, try manual parsing
        if date_obj is None:
            # Try different formats
            formats = [
                "%a, %d %b %Y %H:%M:%S %Z",
                "%a, %d %b %Y %H:%M:%S GMT",
                "%a, %d %b %Y %H:%M:%S",
                "%Y-%m-%dT%H:%M:%S%z",
                "%Y-%m-%dT%H:%M:%S",
                "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%d",
                "%d %b %Y %H:%M:%S",
                "%d/%m/%Y %H:%M:%S",
            ]
            
            for fmt in formats:
                try:
                    if len(date_str) >= 19:
                        date_obj = datetime.strptime(date_str[:19], fmt)
                    else:
                        date_obj = datetime.strptime(date_str, fmt)
                    break
                except ValueError:
                    continue
        
        if date_obj is None:
            # If still can't parse, return original
            return date_str
        
        # If date_obj is naive (no timezone info), assume it's UTC/GMT
        if date_obj.tzinfo is None:
            # Assume UTC if no timezone specified
            date_obj = date_obj.replace(tzinfo=UTC)
        
        # Convert to Tehran timezone (Asia/Tehran)
        tehran_tz = gettz('Asia/Tehran')
        date_obj_tehran = date_obj.astimezone(tehran_tz)
        
        # Convert to naive datetime for jdatetime (jdatetime doesn't support timezone)
        date_obj_naive = date_obj_tehran.replace(tzinfo=None)
        
        # Convert to Persian (Jalali)
        persian_date = jdatetime.datetime.fromgregorian(datetime=date_obj_naive)
        return persian_date.strftime("%Y/%m/%d %H:%M")
    except Exception as e:
        # Log error for debugging
        logger.warning(f"Error converting date '{date_str}' to Persian: {e}", exc_info=True)
        # Return original on error
        return date_str

