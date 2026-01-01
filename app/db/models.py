"""Database models for news platform."""

import uuid
from datetime import datetime
from typing import Optional

from sqlalchemy import String, Text, Boolean, Integer, DateTime, UniqueConstraint, Column
from sqlalchemy.sql import func
from sqlalchemy.dialects.postgresql import UUID

from app.db.base import Base


class NewsSource(Base):
    """Model for news source configuration."""

    __tablename__ = "news_sources"

    name = Column(String(100), primary_key=True, nullable=False)
    interval_minutes = Column(Integer, default=300, nullable=False)
    last_run_at = Column(DateTime(timezone=True), nullable=True)
    enabled = Column(Boolean, default=True, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), onupdate=func.now(), nullable=True)


class News(Base):
    """Model for news articles."""

    __tablename__ = "news"

    # Use database-agnostic UUID handling
    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()), nullable=False)
    source = Column(String(100), nullable=False, index=True)
    title = Column(String(500), nullable=False)
    body_html = Column(Text, nullable=True)
    summary = Column(Text, nullable=True)
    url = Column(String(1000), nullable=False, unique=True, index=True)
    published_at = Column(String(100), nullable=True)  # Raw string from source
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False, index=True)
    image_url = Column(String(1000), nullable=True)
    category = Column(String(200), nullable=True, index=True)  # Normalized category
    raw_category = Column(String(200), nullable=True)  # Original category from source

    __table_args__ = (
        UniqueConstraint("url", name="uq_news_url"),
    )

