import pytest
import asyncio
from bnw_scrapie import scrape
from playwright.async_api import async_playwright

# Example player profile URL (replace with a real one if needed)
TEST_PROFILE_URL = "https://baseballnorthwest.com/profiles/12345#player-bar-year"

@pytest.mark.asyncio
async def test_scrape_player_profile():
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        page = await browser.new_page()
        rows = await scrape(page, TEST_PROFILE_URL)
        await browser.close()
    # The result should be a list (possibly empty if the profile doesn't exist)
    assert isinstance(rows, list)
    if rows:
        # Check that required keys exist in the first row
        row = rows[0]
        for key in ["grad_year", "name", "profile_url", "timestamp"]:
            assert key in row
        assert row["profile_url"].startswith("https://baseballnorthwest.com/profiles/")
