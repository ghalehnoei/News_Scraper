You are implementing a SIMPLE READ-ONLY UI
on top of an existing FastAPI News Ingestion API.

This is an auxiliary UI for newsroom preview purposes.

========================
STRICT RULES
========================
- READ-ONLY
- No authentication
- No scraping
- No database writes
- No business logic changes
- API behavior must remain unchanged

========================
GOAL
========================
Create a minimal web UI that displays latest news
in a puzzle / grid layout using server-side rendering.

========================
TECH STACK
========================
- FastAPI (existing)
- Jinja2 templates
- Plain HTML + CSS
- No JavaScript frameworks
- Optional minimal JS only for UX

========================
ROUTES TO ADD
========================
GET /ui/news
- Displays latest news in grid layout
- Fetches data from internal API logic (NOT HTTP call)

GET /ui/news/{id}
- Displays full article
- Shows:
    - title
    - source
    - image
    - body_html (rendered safely)
    - published_at

========================
UI REQUIREMENTS
========================
Grid / puzzle layout:
- Responsive
- 2–4 columns depending on screen width
- Card design:
    - Image on top
    - Title below
    - Source + date footer

========================
IMAGE HANDLING
========================
- Images served via S3 public URL or proxy endpoint
- Broken images handled gracefully

========================
TEMPLATES STRUCTURE
========================
/app/templates/
 ├── base.html
 ├── news_grid.html
 └── news_detail.html

/app/static/
 └── styles.css

========================
SECURITY
========================
- Sanitize rendered HTML body
- Do NOT allow script execution
- No user input accepted

========================
QUALITY BAR
========================
- Clean HTML
- Simple CSS Grid
- Readable templates
- No inline styles
- No frontend build tools

========================
DELIVERABLES
========================
- Working UI at /ui/news
- News cards rendered correctly
- Clicking card opens detail page
- API remains unchanged

========================
FINAL RULE
========================
This UI is intentionally simple and disposable.
Do NOT over-engineer.
Focus on clarity and newsroom usability.
