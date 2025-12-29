You are extending the Persian News Ingestion Platform
by adding a new continuous worker for IRNA News Agency.

MehrNews and ISNA workers already exist and are stable.

========================
ABSOLUTE RULES
========================
- Do NOT modify Mehr or ISNA workers
- Do NOT refactor shared code
- Follow existing worker patterns exactly
- IRNA logic must be isolated

========================
SOURCE DETAILS
========================
Source name: irna
RSS feed (main):
https://www.irna.ir/rss

(If multiple feeds exist, start with the main feed only.)

========================
WORKER MODEL
========================
- Continuous daemon worker
- One worker = IRNA only
- Poll interval configurable
- Independent execution

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
From IRNA article HTML extract:
- title
- full article body (HTML)
- summary (if present)
- category (raw category preserved)

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
CATEGORY HANDLING
========================
- Store raw_category from IRNA
- Apply existing category normalization logic
- Unknown categories → \"other\"

========================
IMAGE EXTRACTION
========================
- Extract only ONE main image
- Prefer og:image
- Skip logos, icons, ads
- Upload image to S3
- Store S3 path in DB

========================
DEDUPLICATION
========================
- Deduplicate strictly by article URL
- If URL exists in DB → skip

========================
RATE LIMIT
========================
- Respect per-source rate limit configuration
- Use IRNA-specific limits

========================
ERROR HANDLING
========================
- Failure of one article must NOT stop worker
- Network retries with timeout
- Log failures with article URL

========================
LOGGING
========================
Logs must include:
- source=irna
- article_url (when applicable)

========================
API & UI IMPACT
========================
- API automatically exposes IRNA news
- UI shows IRNA news without modification
- Source and category filters must work

========================
DELIVERABLES
========================
- IRNA worker implementation
- Continuous scraping working
- Articles stored in DB
- Images stored in S3
- No regression in Mehr and ISNA

========================
QUALITY BAR
========================
- Async HTTP
- Clean selectors
- Clear docstrings
- No TODOs
- No unnecessary duplication

========================
FINAL RULE
========================
IRNA must integrate seamlessly as a first-class source
while preserving system stability.
