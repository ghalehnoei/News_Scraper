FROM python:3.11-slim

WORKDIR /app

# Install system dependencies for Python packages
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Install Playwright browsers (Chromium)
RUN playwright install chromium

# Install system dependencies for Chromium
# This installs all required system libraries for Chromium to run
RUN playwright install-deps chromium

# Copy application code
COPY app/ ./app/

# Run worker
CMD ["python", "-m", "app.workers.runner"]

