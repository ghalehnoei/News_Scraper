You are extending the Persian News Ingestion Platform.

This phase implements CATEGORY NORMALIZATION
to unify categories across different news sources.

========================
STRICT RULES
========================
- No NLP
- No AI classification
- No UI-side logic
- No breaking existing API behavior
- Minimal database change allowed

========================
GOAL
========================
Ensure that different category labels from different sources
map into a unified, newsroom-friendly category set.

========================
CATEGORY STRATEGY
========================
Each article must store:
1. raw_category   → original category from source
2. category       → normalized category

UI and API MUST use normalized category only.

========================
NORMALIZED CATEGORY SET
========================
Define a fixed enum-like set, e.g.:

- politics
- economy
- society
- international
- culture
- sports
- science
- technology
- health
- other

========================
MAPPING RULES
========================
Implement a deterministic mapping dictionary per source.

Example (illustrative):
- "سیاسی" → politics
- "سیاست" → politics
- "سیاست داخلی" → politics
- "بین‌الملل" → international
- "اقتصادی" → economy
- "اجتماعی" → society

Mapping must:
- Be explicit
- Be configurable
- Be easy to extend

========================
IMPLEMENTATION LOCATION
========================
- Category normalization MUST happen in worker layer
- Before inserting article into database
- API and UI must remain unchanged

========================
DATABASE CHANGES
========================
- Add column: raw_category (string)
- category column stores normalized value
- Existing data migration not required (can be NULL)

========================
CONFIGURATION
========================
- Category mappings stored as Python dicts
- One mapping file per source OR shared mapping file

========================
ERROR HANDLING
========================
- Unknown categories → map to "other"
- Log unmapped categories for later review

========================
LOGGING
========================
Logs must include:
- source
- raw_category
- normalized_category

========================
DELIVERABLES
========================
- Normalized categories stored in DB
- UI category filter works consistently
- No regression in existing functionality

========================
FINAL RULE
========================
This normalization layer is foundational.
Design it cleanly so future NLP-based categorization
can replace it without breaking contracts.
