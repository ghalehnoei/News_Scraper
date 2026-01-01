"""Database models for news platform."""

import uuid
from datetime import datetime
from typing import Optional

from sqlalchemy import String, Text, Boolean, Integer, DateTime, UniqueConstraint, Column, TypeDecorator
from sqlalchemy.sql import func
from sqlalchemy.dialects.postgresql import UUID as PG_UUID

from app.db.base import Base
from app.core.config import settings


class GUID(TypeDecorator):
    """
    Platform-independent GUID type.
    Uses PostgreSQL's UUID type when using PostgreSQL,
    otherwise uses String(36) for SQLite and other databases.
    """
    impl = String
    cache_ok = True

    def load_dialect_impl(self, dialect):
        if dialect.name == 'postgresql':
            # Use UUID type for PostgreSQL (as_uuid=True means it works with UUID objects)
            return dialect.type_descriptor(PG_UUID(as_uuid=True))
        else:
            # Use String for SQLite and other databases
            return dialect.type_descriptor(String(36))

    def process_bind_param(self, value, dialect):
        if value is None:
            return value
        elif dialect.name == 'postgresql':
            # For PostgreSQL, convert string to UUID object
            if isinstance(value, uuid.UUID):
                return value
            elif isinstance(value, str):
                try:
                    return uuid.UUID(value)
                except (ValueError, AttributeError):
                    # If invalid UUID string, try to generate one or return None
                    return None
            else:
                return uuid.UUID(str(value))
        else:
            # For SQLite, keep as string
            if isinstance(value, uuid.UUID):
                return str(value)
            if not isinstance(value, str):
                return str(value)
            return value

    def process_result_value(self, value, dialect):
        if value is None:
            return value
        elif dialect.name == 'postgresql':
            # PostgreSQL returns UUID object, convert to string
            if isinstance(value, uuid.UUID):
                return str(value)
            return str(value)
        else:
            # SQLite returns string
            return str(value)


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
    # GUID type automatically uses UUID for PostgreSQL and String for SQLite
    id = Column(GUID(), primary_key=True, default=lambda: str(uuid.uuid4()), nullable=False)
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

