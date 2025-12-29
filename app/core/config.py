"""Configuration management for the application."""

from typing import Optional
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    # Database
    database_url: str = "postgresql+asyncpg://postgres:postgres@postgres:5432/news_db"

    # S3 Storage
    s3_endpoint: str = "http://minio:9000"
    s3_bucket: str = "news-images"
    s3_access_key: str = "minioadmin"
    s3_secret_key: str = "minioadmin"
    s3_region: str = "us-east-1"
    s3_use_ssl: bool = False
    s3_verify_ssl: bool = True  # Set to False to disable SSL certificate verification

    # Worker
    worker_source: Optional[str] = None
    poll_interval: int = 300  # 5 minutes default
    
    # Rate Limiting (per source)
    max_requests_per_minute: int = 60  # Maximum requests per minute per source
    delay_between_requests: float = 1.0  # Minimum delay in seconds between requests

    # API
    api_host: str = "0.0.0.0"
    api_port: int = 3000

    # Logging
    log_level: str = "INFO"

    model_config = SettingsConfigDict(
        case_sensitive=False,
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",  # Ignore extra fields from .env that are not in the model
    )


settings = Settings()

