# Implementation Plan - Enhanced Worker Architecture

## Overview

This document outlines the implementation plan for refactoring the news scraping platform to use an enhanced worker hierarchy with shared abstractions.

## Current State Analysis

### Key Findings from Code Review:

1. **BaseWorker** - Provides lifecycle management, signal handling, and polling loop
2. **AFPTextWorker** - API-based worker with authentication, rate limiting, and article processing
3. **TasnimWorker** - Web scraping worker with HTML parsing and retry logic
4. **RateLimiter** - Already implemented and working well
5. **Category Normalizer** - Comprehensive mappings for Persian news sources
6. **Database Models** - News and NewsSource models with proper indexing

### Common Patterns Identified:

1. **HTTP Session Management** - Duplicated in multiple workers
2. **Rate Limiting** - Already centralized via RateLimiter class
3. **Authentication** - Varies by worker (AFP uses OAuth, others may use different methods)
4. **Article Processing** - Similar logic for building HTML, normalizing categories
5. **Database Operations** - Repeated CRUD patterns

## Implementation Strategy

### Phase 1: Foundation Classes (High Priority)

#### 1. APIWorker Class
**File:** `app/workers/api_worker.py`

**Responsibilities:**
- HTTP session management
- API authentication patterns
- Rate limiting integration
- Error handling for API responses
- Common API request methods (GET, POST)

**Key Methods:**
- `_get_http_session()` - Create/reuse HTTP session
- `_authenticate()` - Abstract authentication (to be overridden)
- `_handle_api_error()` - Standardize error responses
- `_make_api_request()` - Generic API request with auth

#### 2. WebScraperWorker Class
**File:** `app/workers/web_scraper_worker.py`

**Responsibilities:**
- HTTP session with scraping-specific headers
- Retry logic with exponential backoff
- HTML content fetching
- Basic HTML parsing utilities
- Rate limiting integration

**Key Methods:**
- `_get_http_session()` - Session with scraping headers
- `_fetch_with_retry()` - Retry logic for HTTP requests
- `_parse_html()` - Basic HTML parsing utilities

#### 3. HTTPClient Class
**File:** `app/services/http_client.py`

**Responsibilities:**
- Centralized HTTP session management
- Retry logic with exponential backoff
- Circuit breaker pattern
- Request/response logging
- Timeout management

**Key Methods:**
- `get()` - HTTP GET request
- `post()` - HTTP POST request
- `close()` - Cleanup resources

#### 4. NewsRepository Class
**File:** `app/services/news_repository.py`

**Responsibilities:**
- All database operations for News model
- Transaction management
- Caching for common queries
- Duplicate detection

**Key Methods:**
- `get_by_url()` - Check if URL exists
- `save()` - Save new article
- `update()` - Update existing article
- `get_recent()` - Get recent articles

#### 5. ArticleProcessor Class
**File:** `app/services/article_processor.py`

**Responsibilities:**
- Category normalization
- HTML body generation
- Date parsing and formatting
- News object creation
- Common article transformations

**Key Methods:**
- `normalize_category()` - Map raw category to normalized
- `build_body_html()` - Create HTML from article data
- `create_news_object()` - Build News model instance
- `parse_date()` - Handle various date formats

### Phase 2: Refactoring Existing Workers

#### Proof of Concept: AFPTextWorker

**Changes:**
1. Inherit from `APIWorker` instead of `BaseWorker`
2. Remove duplicate HTTP session management
3. Use `NewsRepository` for database operations
4. Use `ArticleProcessor` for article processing
5. Keep source-specific logic (authentication, API endpoints)

**Benefits:**
- ~30% reduction in code
- More maintainable
- Easier to test

#### Proof of Concept: TasnimWorker

**Changes:**
1. Inherit from `WebScraperWorker` instead of `BaseWorker`
2. Use `HTTPClient` for HTTP operations
3. Use `NewsRepository` for database operations
4. Use `ArticleProcessor` for article processing
5. Keep source-specific parsing logic

**Benefits:**
- Consistent patterns
- Better error handling
- Easier to extend

### Phase 3: Testing and Validation

1. **Unit Tests** - Test each abstraction in isolation
2. **Integration Tests** - Test worker lifecycle
3. **Performance Tests** - Ensure no degradation
4. **Backward Compatibility** - Verify old and new can coexist

## Migration Path

### Step 1: Create Foundation Classes
- Implement `APIWorker`, `WebScraperWorker`, `HTTPClient`, `NewsRepository`, `ArticleProcessor`
- Add comprehensive tests

### Step 2: Refactor Workers Incrementally
- Start with AFPTextWorker (API-based)
- Then TasnimWorker (web scraping)
- Continue with other workers

### Step 3: Update Runner
- Ensure runner can instantiate new worker types
- Maintain backward compatibility

### Step 4: Documentation
- Update README with new patterns
- Add examples for creating new workers
- Document migration guide

## Risk Mitigation

### Potential Risks:
1. **Breaking existing functionality** - Mitigated by incremental refactoring
2. **Performance degradation** - Mitigated by benchmarking
3. **Increased complexity** - Mitigated by keeping abstractions simple
4. **Resistance to change** - Mitigated by clear documentation of benefits

### Benefits of Refactoring:
1. **Reduced Code Duplication** - ~30-40% reduction
2. **Improved Maintainability** - Changes in one place
3. **Better Testability** - Clear separation of concerns
4. **Consistent Behavior** - All workers follow same patterns
5. **Easier Onboarding** - New developers understand structure quickly
6. **Reduced Bugs** - Centralized error handling and validation

## Next Steps

1. ✅ Analyze current codebase
2. ✅ Create implementation plan
3. ⏳ Implement foundation classes (APIWorker, WebScraperWorker, HTTPClient, NewsRepository, ArticleProcessor)
4. ⏳ Refactor AFPTextWorker as proof of concept
5. ⏳ Refactor TasnimWorker as proof of concept
6. ⏳ Update runner and documentation
7. ⏳ Test and validate
8. ⏳ Refactor remaining workers
