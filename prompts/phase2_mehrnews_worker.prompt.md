You are implementing PHASE 2 of a Persian News Ingestion Platform.

The project already contains a working skeleton (Phase 1).
You MUST strictly follow the existing structure and the PRD.

========================
PHASE 2 GOAL
========================
Implement the FIRST real continuous worker for:
- Source: Mehr News Agency
- Input: RSS feed
- Output: Database + S3

This worker will serve as the reference implementation for all future sources.

========================
ABSOLUTE RULES
========================
- Modify ONLY files related to MehrNews
- Do NOT refactor unrelated code
- Do NOT introduce abstractions for future sources yet
- Do NOT add other news agencies
- Follow existing DB models and S3 client
- Production-grade code only

========================
SOURCE DETAILS
========================
Source name: mehrnews
RSS URL:
https://www.mehrnews.com/rss

========================
WORKER BEHAVIOR
========================
- Worker runs continuously
- Polls RSS feed every N minutes (from config or DB)
- On each poll:
    1. Fetch RSS
    2. Parse items
    3. Detect new articles using URL uniqueness
    4. For each new article:
        - Fetch article page
        - Extract title
        - Extract full article body (HTML)
        - Extract summary
        - Extract category (if exists)
        - Extract main image URL (if exists)
        - Download image
        - Upload image to S3
        - Store article in DB

========================
RSS FIELDS
========================
From RSS item:
- title
- link
- description
- pubDate
- category (optional)

========================
ARTICLE PAGE EXTRACTION
========================
- Prefer <article> tag
- Fallback selectors if needed
- Remove script/style tags
- Preserve HTML structure
- Do NOT embed image tags into body

========================
IMAGE RULES
========================
- Only ONE main image
- Skip logos, icons, ads
- Prefer og:image
- Upload to S3 bucket
- Store S3 path in DB

========================
DEDUPLICATION (SCOPE LIMITED)
========================
- Dedup ONLY by article URL
- If URL already exists in DB → skip

========================
ERROR HANDLING
========================
- Failure of one article must NOT stop worker
- Network retries with timeout
- Log errors with article URL

========================
LOGGING
========================
Each log must include:
- source=mehrnews
- article_url (if applicable)

========================
API IMPACT
========================
- API remains unchanged
- /news/latest should now return MehrNews articles

========================
DELIVERABLES
========================
- Working MehrNews worker
- Articles stored in DB
- Images stored in S3
- API returns real data
- Worker survives multiple polling cycles

========================
QUALITY BAR
========================
- Async HTTP (aiohttp or httpx)
- Clean selectors
- No hardcoded paths
- Clear docstrings
- No TODOs

========================
FINAL RULE
========================
This implementation becomes the template for all future sources.
Keep it clean, explicit, and readable.
