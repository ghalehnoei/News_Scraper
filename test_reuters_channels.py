"""Test script to check Reuters text channels API."""

import asyncio
import sys
import os

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.sources.reuters_text import ReutersTextWorker
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


async def test_channels():
    """Test fetching Reuters text channels."""
    print("=== Testing Reuters Text Channels ===\n")

    # Create worker instance
    worker = ReutersTextWorker()

    # Authenticate first
    print("1. Authenticating...")
    if not await worker._authenticate():
        print("[FAIL] Authentication failed")
        return
    print(f"[OK] Authenticated. Token: {worker.auth_token[:20]}...\n")

    # Fetch channels
    print("2. Fetching text channels...")
    channels_xml = await worker._fetch_text_channels()
    if not channels_xml:
        print("[FAIL] Failed to fetch channels XML")
        return

    print("[OK] Channels XML fetched\n")

    # Parse channels
    print("3. Parsing channels...")
    channels = worker._parse_channels_list(channels_xml)
    if not channels:
        print("[FAIL] No channels parsed")
        return

    print(f"[OK] Found {len(channels)} channels:")
    for i, channel in enumerate(channels[:10]):  # Show first 10
        print(f"  {i+1}. {channel['alias']} - {channel['description']}")

    if len(channels) > 10:
        print(f"  ... and {len(channels) - 10} more")

    # Clean up
    await worker.close()
    print("\n[OK] Test Complete")


if __name__ == "__main__":
    asyncio.run(test_channels())

