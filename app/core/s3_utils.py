"""S3 URL processing utilities."""

import logging
from typing import Optional
from urllib.parse import urlparse, urlunparse, unquote

from app.core.config import settings
from app.storage.s3 import generate_presigned_url

logger = logging.getLogger(__name__)


def extract_s3_key_from_url(url: str) -> Optional[str]:
    """
    Extract S3 key from various URL formats.
    
    Supports:
    - s3://bucket/path format
    - {endpoint}/{bucket}/{key} format
    - Relative path (no scheme/netloc)
    - Direct S3 key
    
    Args:
        url: URL in various formats
        
    Returns:
        S3 key if extraction successful, None otherwise
    """
    if not url:
        return None
    
    try:
        # Remove query string and fragment from URL before processing
        parsed = urlparse(url)
        
        # Decode URL-encoded path
        decoded_path = unquote(parsed.path)
        
        # Remove query string from decoded path if it exists
        if '?' in decoded_path:
            decoded_path = decoded_path.split('?')[0]
        
        # Reconstruct URL without query string and fragment
        clean_url = urlunparse((parsed.scheme, parsed.netloc, decoded_path, '', '', ''))
        
        # Extract S3 key from URL
        if clean_url.startswith("s3://"):
            # Parse s3://bucket/path format
            path_after_s3 = clean_url[5:]  # Remove "s3://"
            parts = path_after_s3.split("/", 1)
            if len(parts) == 2:
                s3_key = parts[1]
            else:
                s3_key = path_after_s3
            logger.debug(f"Extracted S3 key from s3:// URL: {s3_key}")
            return s3_key
        elif clean_url.startswith(settings.s3_endpoint):
            # Remove endpoint and bucket to get the key
            prefix = f"{settings.s3_endpoint}/{settings.s3_bucket}/"
            if clean_url.startswith(prefix):
                s3_key = clean_url[len(prefix):]
            else:
                # Try without trailing slash
                prefix = f"{settings.s3_endpoint}/{settings.s3_bucket}"
                if clean_url.startswith(prefix):
                    s3_key = clean_url[len(prefix):].lstrip("/")
                else:
                    # Path might have bucket name duplicated, try to extract key
                    # Example: https://gpmedia.nrms.ir/news-images/news-images/mashreghnews/...
                    path_parts = decoded_path.lstrip("/").split("/")
                    if len(path_parts) > 1 and path_parts[0] == path_parts[1]:
                        # Bucket name is duplicated, skip first occurrence
                        s3_key = "/".join(path_parts[1:])
                    else:
                        s3_key = decoded_path.lstrip("/")
            return s3_key
        elif not parsed.scheme and not parsed.netloc:
            # Relative path (no scheme, no netloc) - assume it's an S3 key
            # Examples: news-images/reuters_video/2026/01/06/2a4f0323.jpg
            s3_key = decoded_path.lstrip("/")
            logger.debug(f"Detected relative path as S3 key: {s3_key}")
            return s3_key
        else:
            # Assume it's already an S3 key
            return clean_url
    except Exception as e:
        logger.warning(f"Error extracting S3 key from URL {url[:100]}: {e}")
        return None


def is_presigned_url(url: str) -> bool:
    """
    Check if URL is already a presigned URL.
    
    Args:
        url: URL to check
        
    Returns:
        True if URL appears to be presigned, False otherwise
    """
    if not url or '?' not in url:
        return False
    return 'X-Amz-Signature' in url or 'signature' in url.lower()


async def convert_image_url_to_presigned(image_url: Optional[str]) -> Optional[str]:
    """
    Convert image URL to presigned URL if needed.
    
    Args:
        image_url: Original image URL (can be S3 key, s3:// URL, or endpoint URL)
        
    Returns:
        Presigned URL if conversion successful, original URL otherwise
    """
    if not image_url:
        return None
    
    # Check if it's already a presigned URL
    if is_presigned_url(image_url):
        return image_url
    
    try:
        # Extract S3 key from URL
        s3_key = extract_s3_key_from_url(image_url)
        if not s3_key:
            return image_url
        
        # Remove any remaining query string from S3 key
        if '?' in s3_key:
            s3_key = s3_key.split('?')[0]
        
        # Generate presigned URL
        presigned = await generate_presigned_url(s3_key)
        if presigned:
            return presigned
    except Exception as e:
        logger.warning(f"Failed to generate presigned URL for {image_url[:100]}: {e}", exc_info=True)
    
    # Return original URL if conversion fails
    return image_url

