You are extending the Persian News Ingestion Platform
by adding a continuous worker for Fars News Agency.

This source does NOT provide a reliable RSS feed.

========================
ABSOLUTE RULES
========================
- Do NOT modify existing workers (Mehr, ISNA, IRNA)
- Do NOT refactor shared code
- Fars logic must be fully isolated
- Follow existing DB and S3 contracts exactly

========================
SOURCE DETAILS
========================
Source name: fars
Primary listing page:
https://farsnews.ir/showcase

========================
WORKER MODEL
========================
- Continuous daemon worker
- One worker = Fars only
- Poll listing page periodically
- Independent execution

========================
LISTING PAGE PROCESSING
========================
From the showcase page:
- Extract article links
- Extract visible title (if present)
- Ignore non-news items
- Normalize URLs to canonical form

========================
NEW ARTICLE DETECTION
========================
- Use article URL as unique identifier
- Compare against database
- Only process unseen URLs

========================
ARTICLE PAGE EXTRACTION
========================
From Fars article HTML extract:
- title
- full article body (HTML)
- summary (if present)
- raw category (if present)
- published date (raw string)

Extraction rules:
- Prefer <article> tag
- Fallback selectors:
    - .nt-body
    - .news-text
    - .content
- Remove script/style tags
- Preserve paragraph structure
- Do NOT inline images into body

========================
CATEGORY HANDLING
========================
- Store raw_category from Fars
- Apply existing category normalization logic
- Unknown categories → \"other\"

========================
IMAGE EXTRACTION
========================
- Extract only ONE main image
- Prefer og:image
- Fallback to article header image
- Skip logos, icons, avatars, ads
- Upload image to S3
- Store S3 path in DB

========================
RATE LIMITING (CRITICAL)
========================
- Apply strict per-source rate limits
- Delay between requests is mandatory
- Handle HTTP 429 gracefully
- Exponential backoff on temporary blocks

========================
DEDUPLICATION
========================
- Deduplicate strictly by URL
- If URL exists in DB → skip immediately

========================
ERROR HANDLING
========================
- One failed article must NOT stop worker
- Network failures retried with backoff
- Parsing failures logged and skipped

========================
LOGGING
========================
All logs must include:
- source=fars
- request_type (listing | article)
- article_url (if applicable)

========================
API & UI IMPACT
========================
- API automatically exposes Fars news
- UI shows Fars news without modification
- Source & category filters must work

========================
DELIVERABLES
========================
- Fars worker implementation
- Continuous scraping working
- Articles stored in DB
- Images stored in S3
- No regression in other sources

========================
QUALITY BAR
========================
- Async HTTP
- Defensive selectors
- Clean, readable code
- Clear docstrings
- No TODOs

========================
FINAL RULE
========================
Fars is the hardest source.
Implement conservatively and defensively.
Stability is more important than speed.
