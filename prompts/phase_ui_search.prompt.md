You are extending the existing FastAPI News API
and its simple newsroom UI.

This phase implements TEXT SEARCH functionality only.

========================
STRICT RULES
========================
- Read-only
- No scraping changes
- No worker changes
- No NLP
- No external search engines
- No breaking existing endpoints

========================
GOAL
========================
Allow newsroom users to search news content
using a simple keyword-based search.

========================
API CHANGES
========================
Extend existing endpoint:

GET /news/latest

Add optional query parameter:
- q (string, search keyword)

Behavior:
- If q is provided:
    - Filter news where keyword appears in:
        - title
        - summary
        - body_html
- Case-insensitive
- Persian text supported via ILIKE

========================
QUERY RULES
========================
- Minimum query length: 2 characters
- Trim whitespace
- Prevent SQL injection
- Escape wildcards safely

========================
DATABASE
========================
- Use ILIKE (PostgreSQL)
- Combine with existing pagination
- Use OR between fields
- Maintain performance with sensible limits

========================
UI CHANGES
========================
- Add search box at top of /ui/news
- Submit via GET
- Preserve query across pagination
- Show \"no results\" message when empty

========================
UX REQUIREMENTS
========================
- Simple input field
- Submit button
- Optional clear button
- No live search
- No JavaScript framework

========================
QUALITY BAR
========================
- Clean SQLAlchemy queries
- Defensive input handling
- Clear docstrings
- No duplicated logic

========================
DELIVERABLES
========================
- Search-enabled API
- Updated UI with search box
- Pagination works with search
- API remains backward compatible

========================
FINAL RULE
========================
This is a simple newsroom search, not a search engine.
Keep it minimal, fast, and maintainable.
