# bnw-scrapie

Athlete data analysis tool for scraping and analyzing player performance data from BNW.

This project automates the collection of athlete testing data (60 YD, 30 YD, Broad Jump, L-Drill, Med Ball, etc.) for high school baseball players (grad years 2025–2028) from the BNW website. It writes one row per player-year, handles duplicate avoidance, and computes percentiles for each drill. The tool is intended for researchers, coaches, and analysts interested in regional player metrics and trends.

## Running Tests

Install test dependencies:

    pip install -r requirements-dev.txt

Run all tests:

    pytest
