You are extending the Continuous Worker system.

This phase introduces RATE LIMITING per news source.

========================
STRICT RULES
========================
- No API changes
- No database schema changes
- No scraping logic rewrite
- No global rate limiter

========================
GOAL
========================
Ensure each source respects its own request limits
to avoid bans and overload.

========================
RATE LIMIT STRATEGY
========================
Apply rate limiting at TWO levels:

1. RSS / listing fetch
2. Article page fetch

========================
CONFIGURATION
========================
Rate limits must be configurable per source via:

- Environment variables OR
- news_sources table

Example:
- MAX_REQUESTS_PER_MINUTE
- DELAY_BETWEEN_REQUESTS_SECONDS

========================
IMPLEMENTATION DETAILS
========================
- Use async-compatible rate limiter
- Apply delay between article fetches
- Respect HTTP 429 responses
- Exponential backoff on temporary failures

========================
ERROR HANDLING
========================
- Rate limit violation must NOT crash worker
- Worker must continue gracefully
- Log rate limit events clearly

========================
LOGGING
========================
Logs must include:
- source
- request_type (rss | article)
- delay_applied

========================
DELIVERABLES
========================
- Per-source rate limiting implemented
- Configurable limits
- Worker stability preserved
- No regression in scraping behavior
