"""
bnw_bar_scraper_incremental.py
==============================
• Grad years 2025-2028
• 1-second pause between “Load More” clicks
• Per-profile throttle: 5-25 s
• Immediate append to bnw_bar_raw.csv after every OK player
• Recomputes bnw_bar_percentiles.csv at the end
"""

import asyncio, random, re, time, numpy as np, pandas as pd
from datetime import datetime
from urllib.parse import urlparse
from pathlib import Path
from playwright.async_api import async_playwright, TimeoutError as PwTimeout
from bs4 import BeautifulSoup

BASE       = "https://baseballnorthwest.com"
YEARS      = ["2028"]
WAIT_MIN   = 5
WAIT_MAX   = 25
LOAD_WAIT  = 1
CF_WAIT    = 60
CF_RETRY   = 2
RAW_FILE   = Path("bnw_bar_raw.csv")

DRILLS = {
    "60 YD":      ("60_time", "60_pct", "60_class_pct", "60_state_pct"),
    "30 YD":      ("30_time", "30_pct", "30_class_pct", "30_state_pct"),
    "Broad Jump": ("broad_ft", "broad_pct", "broad_class_pct", "broad_state_pct"),
    "L-Drill":    ("l_time", "l_pct", "l_class_pct", "l_state_pct"),
    "Med Ball":   ("med_ft", "med_pct", "med_class_pct", "med_state_pct"),
}

# ── utility helpers ───────────────────────────────────────────────────────
log            = lambda m: print(time.strftime("%H:%M:%S"), m, flush=True)
grad_url       = lambda y: f"{BASE}/find-player?graduation_year={y}&submit=Submit"
tidy           = lambda s: s.strip().replace("\xa0", "")
slug_to_name   = lambda s: s.replace("-", " ").title()
looks_like_cf  = lambda h: ("Attention Required!" in h) or ('cf-error-details' in h)

def to_percent(val: str):
    if not val: return np.nan
    val = val.strip()
    if val.startswith("<"):
        try:  return float(val[1:]) - 0.01
        except ValueError: return np.nan
    try:  return float(val)
    except ValueError: return np.nan

def parse_stat(div):
    label = div.find("h4").get_text(strip=True)
    if label not in DRILLS: return {}
    score = tidy(div.select_one("div.stat-value").text)
    ranks = {rp["data-type"]: tidy(rp.text).replace("%","").replace("< ","<")
             for rp in div.select("div.rank-percentile")}
    sc, po, pc, ps = DRILLS[label]
    return {
        sc: score,
        po: to_percent(ranks.get("overall", "")),
        pc: to_percent(ranks.get("graduation_year", "")),
        ps: to_percent(ranks.get("state", "")),
    }

# ── profile scraping ─────────────────────────────────────────────────────
async def fetch_html(page, url):
    url = url.split("#")[0] + "#player-bar-year"
    for _ in range(CF_RETRY + 1):
        await page.goto(url, timeout=45_000)
        html = await page.content()
        if not looks_like_cf(html): return html
        log(f"   Cloudflare – wait {CF_WAIT}s"); await asyncio.sleep(CF_WAIT)
    return html

async def scrape(page, url):
    soup = BeautifulSoup(await fetch_html(page, url), "html.parser")
    if not soup.select_one("div.player-stats"): return None

    # ensure BAR items attached
    if not soup.select_one("#player-bar-year div.stat-item"):
        try:
            await page.wait_for_selector("#player-bar-year div.stat-item",
                                         state="attached", timeout=3000)
            soup = BeautifulSoup(await page.content(), "html.parser")
        except PwTimeout:
            return None

    # name & grad
    slug  = urlparse(url).path.split("/")[-1]
    name  = slug_to_name(slug)
    sel   = soup.select_one("#player-bar-year select.purei-bar-filter-select")
    grad  = sel.get("data-graduation_year", "0000") if sel else "0000"

    data = {}
    for block in soup.select("#player-bar-year div.stat-item"):
        data.update(parse_stat(block))

    # ── FIXED missing-column check ────────────────────────────────────
    missing = [vals[0] for vals in DRILLS.values() if vals[0] not in data]
    if missing: return None

    data.update(
        name=name,
        grad_year=grad,
        profile_url=url,
        timestamp=datetime.utcnow().isoformat(timespec="seconds"),
    )
    return data

# ── collect profile links for a grad year ────────────────────────────────
async def collect_year(page, year):
    log(f"-- Collecting {year} list --")
    await page.goto(grad_url(year), timeout=45_000)
    urls, prev = set(), 0
    while True:
        for a in await page.locator("a[href^='/profiles/']").element_handles():
            href = await a.get_attribute("href")
            if href: urls.add(BASE + href)
        btn = page.locator("button:has-text('Load More')")
        if not await btn.count(): break
        if not (await btn.first.is_visible() and await btn.first.is_enabled()):
            break
        try: await btn.first.click(timeout=5000)
        except (PwTimeout, asyncio.CancelledError):
            log("   Load More click failed – stop list."); break
        await asyncio.sleep(LOAD_WAIT)
        if len(urls) == prev: break
        prev = len(urls)
    log(f"Collected {len(urls):,} links for {year}")
    return urls

# ── main orchestration ───────────────────────────────────────────────────
async def main():
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        page    = await browser.new_page()

        links = set()
        for y in YEARS:
            links.update(await collect_year(page, y))
        log(f"Total unique links: {len(links):,}")

        seen = set()
        for idx, url in enumerate(links, 1):
            log(f"[{idx}/{len(links)}] {url}")
            row = await scrape(page, url)

            wait = random.uniform(WAIT_MIN, WAIT_MAX)
            if row and row["name"] not in seen:
                seen.add(row["name"])
                # append immediately
                first = not RAW_FILE.exists()
                pd.DataFrame([row]).to_csv(
                    RAW_FILE, mode="a", index=False, header=first
                )
                log(f"   wrote {row['name']}")
            else:
                log("   skip")
            log(f"   wait {wait:0.1f}s"); await asyncio.sleep(wait)

        await browser.close(); log("Browser closed.")

    if not RAW_FILE.exists():
        log("No rows saved – exiting."); return

    # recompute percentiles
    df  = pd.read_csv(RAW_FILE)
    idx = list(range(25, 100, 5)) + [99]
    grid = {}

    for drill, (col, *_p) in DRILLS.items():
        ser = pd.to_numeric(                           # ← patched line
            df[col].astype(str).str.replace("'", ".").str.replace('"', ""),
            errors="coerce"
        ).dropna()
        grid[drill.replace(" ", "_")] = np.percentile(ser, idx).round(2)

    pd.DataFrame(grid, index=idx).rename_axis("Percentile") \
        .to_csv("bnw_bar_percentiles.csv")
    log("bnw_bar_percentiles.csv saved")
    log("=== done ===")

if __name__ == "__main__":
    asyncio.run(main())
