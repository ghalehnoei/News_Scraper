You are extending the READ-ONLY FastAPI News API.

This phase implements PAGINATION only.
No other behavior changes are allowed.

========================
STRICT RULES
========================
- Read-only
- No scraping changes
- No worker changes
- No UI redesign (only adapt to pagination)
- No breaking existing API behavior

========================
GOAL
========================
Add proper pagination support to the news API
to allow newsroom tools to fetch news incrementally.

========================
API CHANGES
========================
Extend existing endpoint:

GET /news/latest

Add query parameters:
- limit (int, default=20, max=100)
- offset (int, default=0)
- source (optional)

========================
RESPONSE FORMAT
========================
Return structured paginated response:

{
  "items": [ ... ],
  "pagination": {
    "limit": 20,
    "offset": 0,
    "total": 12450,
    "has_more": true
  }
}

========================
DATABASE
========================
- Use efficient LIMIT / OFFSET queries
- Order by created_at DESC
- Count query optimized

========================
UI ADAPTATION
========================
- Update UI grid to support pagination
- Simple "Load more" button OR page navigation
- No infinite scroll

========================
QUALITY BAR
========================
- Input validation
- Sensible defaults
- Defensive max limits
- Clear docstrings

========================
DELIVERABLES
========================
- Paginated API endpoint
- Updated UI working with pagination
- No regression in existing behavior
