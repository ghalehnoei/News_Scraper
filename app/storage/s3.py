"""S3-compatible storage client wrapper."""

from typing import Optional

import aioboto3
from botocore.config import Config
from botocore.exceptions import ClientError

from app.core.config import settings
from app.core.logging import setup_logging

logger = setup_logging()

# Global session
_session: Optional[aioboto3.Session] = None


async def init_s3() -> None:
    """Initialize S3 client and test connection."""
    global _session

    _session = aioboto3.Session()

    # Test connection using async context manager
    try:
        # Prepare botocore config for SSL verification
        endpoint_uses_https = settings.s3_endpoint.startswith("https://")
        if endpoint_uses_https and not settings.s3_verify_ssl:
            # Disable SSL verification using Config
            boto_config = Config(
                connect_timeout=60,
                read_timeout=60,
                retries={'max_attempts': 3}
            )
            logger.warning("SSL certificate verification is disabled for S3 connection")
            logger.warning(f"Connecting to: {settings.s3_endpoint}")
        else:
            boto_config = Config(
                connect_timeout=60,
                read_timeout=60,
                retries={'max_attempts': 3}
            )
        
        # Prepare client config
        client_kwargs = {
            "endpoint_url": settings.s3_endpoint,
            "aws_access_key_id": settings.s3_access_key,
            "aws_secret_access_key": settings.s3_secret_key,
            "region_name": settings.s3_region,
            "use_ssl": settings.s3_use_ssl,
            "config": boto_config,
        }
        
        # Set verify parameter for SSL verification control
        if endpoint_uses_https:
            client_kwargs["verify"] = settings.s3_verify_ssl
        
        async with _session.client("s3", **client_kwargs) as s3_client:
            # Test connection
            try:
                await s3_client.head_bucket(Bucket=settings.s3_bucket)
                logger.info(f"S3 connection successful to bucket: {settings.s3_bucket}")
            except ClientError as e:
                error_code = e.response.get("Error", {}).get("Code", "Unknown")
                if error_code == "404":
                    # Bucket doesn't exist, try to create it
                    try:
                        await s3_client.create_bucket(Bucket=settings.s3_bucket)
                        logger.info(f"Created S3 bucket: {settings.s3_bucket}")
                    except ClientError as create_error:
                        logger.error(f"Failed to create S3 bucket: {create_error}")
                        raise
                else:
                    logger.error(f"S3 connection test failed: {e}")
                    raise
    except Exception as e:
        logger.error(f"Unexpected error testing S3 connection: {e}")
        raise


async def close_s3() -> None:
    """Close S3 client connections."""
    global _session

    if _session:
        _session = None

    logger.info("S3 connections closed")


def get_s3_session() -> aioboto3.Session:
    """
    Get the S3 session instance.

    Returns:
        S3 session instance

    Raises:
        RuntimeError: If S3 session is not initialized
    """
    if _session is None:
        raise RuntimeError("S3 session not initialized. Call init_s3() first.")
    return _session


async def generate_presigned_url(s3_path: str, expiration: int = 3600) -> Optional[str]:
    """
    Generate a presigned URL for an S3 object.

    Args:
        s3_path: S3 object path or full URL
                  - Path format: "news-images/mehrnews/2025/12/27/image.jpg"
                  - Full URL format: "{endpoint}/{bucket}/{path}"
        expiration: URL expiration time in seconds (default: 1 hour)

    Returns:
        Presigned URL or None if generation failed
    """
    if _session is None:
        logger.error("S3 session not initialized")
        return None

    try:
        bucket = settings.s3_bucket
        
        # Extract S3 key from path or URL
        if s3_path.startswith("s3://"):
            # Parse s3://bucket/path format
            # Remove "s3://" prefix
            path_after_s3 = s3_path[5:]  # Remove "s3://"
            # Split bucket and key
            parts = path_after_s3.split("/", 1)
            if len(parts) == 2:
                path_bucket, key = parts
                # Use the path after bucket as key (regardless of bucket name)
                key = key
                logger.debug(f"Extracted S3 key from s3:// URL: {key} (bucket: {path_bucket})")
            else:
                # No path after bucket, this is invalid but use as-is
                key = path_after_s3
                logger.warning(f"Invalid s3:// URL format (no path after bucket): {s3_path}")
        elif s3_path.startswith("http"):
            # Full URL format: extract the key (path after bucket)
            from urllib.parse import urlparse
            parsed = urlparse(s3_path)
            # Remove leading slash and extract path
            path = parsed.path.lstrip("/")
            # Remove bucket name if present in path
            if path.startswith(f"{bucket}/"):
                key = path[len(f"{bucket}/"):]
            else:
                # Assume the path is the key
                key = path
        else:
            # Assume it's already an S3 key
            key = s3_path

        endpoint_uses_https = settings.s3_endpoint.startswith("https://")
        
        from botocore.config import Config
        boto_config = Config(
            connect_timeout=60,
            read_timeout=60,
            retries={'max_attempts': 3}
        )
        
        client_kwargs = {
            "endpoint_url": settings.s3_endpoint,
            "aws_access_key_id": settings.s3_access_key,
            "aws_secret_access_key": settings.s3_secret_key,
            "region_name": settings.s3_region,
            "use_ssl": settings.s3_use_ssl,
            "config": boto_config,
        }
        if endpoint_uses_https:
            client_kwargs["verify"] = settings.s3_verify_ssl

        async with _session.client("s3", **client_kwargs) as s3_client:
            presigned_url = await s3_client.generate_presigned_url(
                "get_object",
                Params={"Bucket": bucket, "Key": key},
                ExpiresIn=expiration
            )
            return presigned_url

    except Exception as e:
        logger.error(f"Error generating presigned URL for {s3_path}: {e}", exc_info=True)
        return None

