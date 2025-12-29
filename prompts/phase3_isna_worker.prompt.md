You are implementing PHASE 3 of the Persian News Ingestion Platform.

Phase 1 (Skeleton) and Phase 2 (MehrNews worker) are complete and stable.

Your task is to ADD a new continuous worker for ISNA News Agency.

========================
ABSOLUTE RULES
========================
- Do NOT modify MehrNews worker behavior
- Do NOT refactor shared code
- Do NOT introduce new abstractions
- Follow MehrNews implementation pattern
- ISNA logic must be isolated

========================
SOURCE DETAILS
========================
Source name: isna
RSS feed (primary):
https://www.isna.ir/rss

(If multiple feeds exist, start with the main feed only.)

========================
WORKER MODEL
========================
- Continuous (daemon)
- One worker = ISNA only
- Poll interval configurable
- Independent from other workers

========================
RSS PROCESSING
========================
From RSS item extract:
- title
- link
- description
- pubDate
- category (if exists)

========================
ARTICLE PAGE EXTRACTION
========================
From ISNA article HTML extract:
- title
- full article body (HTML)
- summary (if present)
- category (if available)

Extraction rules:
- Prefer <article> tag
- Fallback selectors:
    - .item-text
    - .news-text
    - .content
- Remove script/style tags
- Preserve paragraph structure
- Do NOT inline images into body

========================
IMAGE EXTRACTION
========================
- Extract only ONE main image
- Prefer:
    - og:image
    - article header image
- Skip:
    - logos
    - icons
    - avatars
- Upload image to S3
- Store S3 path in DB

========================
DEDUPLICATION
========================
- Deduplicate strictly by URL
- If URL already exists in DB → skip

========================
RATE LIMIT
========================
- Respect existing per-source rate limiting mechanism
- Use ISNA-specific limits via config

========================
ERROR HANDLING
========================
- One failed article must NOT stop worker
- Network retries with timeout
- Log failures with article URL

========================
LOGGING
========================
All logs must include:
- source=isna
- article_url (when applicable)

========================
API & UI IMPACT
========================
- API automatically exposes ISNA news
- UI shows ISNA news without modification
- Source filter (if present) must work

========================
DELIVERABLES
========================
- isna worker implementation
- Continuous scraping working
- Data stored in DB
- Images stored in S3
- No regression in MehrNews

========================
QUALITY BAR
========================
- Async HTTP
- Clean selectors
- Clear docstrings
- No TODOs
- No duplication of Mehr code beyond what is necessary

========================
FINAL RULE
========================
ISNA must behave as a first-class source
while keeping the system stable and predictable.
