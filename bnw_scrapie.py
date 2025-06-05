"""
bnw_bar_scraper_incremental.py
==============================
• Grad years 2025-2028
• Writes one row per player-year (blank BAR cols if no data)
• Columns in this exact order:
    grad_year, name, city_state, high_school, positions, bat_throw,
    height, weight, summer_team,
    test_year, 60_time, 60_pct, 60_class_pct, 60_state_pct,
               30_time, 30_pct, 30_class_pct, 30_state_pct,
               broad_ft, broad_pct, broad_class_pct, broad_state_pct,
               l_time,   l_pct,   l_class_pct,   l_state_pct,
               med_ft,   med_pct, med_class_pct, med_state_pct,
    profile_url, timestamp
• Jitter delay 2-10 s, cool-down 120 s every 40 OK rows
• Immediate append to bnw_bar_raw.csv (flushed)
• Skips duplicates across runs by (name, test_year)
• Recomputes bnw_bar_percentiles.csv at the end
"""
import asyncio, random, re, time, os, numpy as np, pandas as pd
from datetime import datetime
from urllib.parse import urlparse
from pathlib import Path
from playwright.async_api import async_playwright, TimeoutError as PwTimeout
from bs4 import BeautifulSoup
import sys

# ─── CONFIG ───────────────────────────────────────────────────────────────
BASE = "https://baseballnorthwest.com"

# Prompt user for grad years
HELP_TEXT = (
    "Usage: python bnw_scrapie.py [YEAR1 YEAR2 ...] or run without arguments and enter years when prompted.\n"
    "Example: python bnw_scrapie.py 2025 2026 2027\n"
    "If no years are provided, you will be prompted to enter them as a comma-separated list (e.g. 2025,2026,2027,2028).\n"
    "Allowed years: 2012 to current year (inclusive).\n"
)
MIN_YEAR = 2012
MAX_YEAR = datetime.now().year

def parse_years(raw_years):
    valid_years = []
    for y in raw_years:
        try:
            y_int = int(y)
            if MIN_YEAR <= y_int <= MAX_YEAR:
                valid_years.append(str(y_int))
            else:
                print(f"Year {y} is out of allowed range ({MIN_YEAR}-{MAX_YEAR}), skipping.")
        except ValueError:
            print(f"Invalid year: {y}, skipping.")
    return valid_years

if any(arg in sys.argv for arg in ('-h', '--help', '/?')):
    print(HELP_TEXT)
    sys.exit(0)

if len(sys.argv) > 1:
    YEARS = parse_years(sys.argv[1:])
else:
    years_input = input(f"Enter graduation years to pull (comma-separated, {MIN_YEAR}-{MAX_YEAR}): ")
    YEARS = parse_years([y.strip() for y in years_input.split(",") if y.strip()])

if not YEARS:
    print(f"No valid years provided. Allowed range: {MIN_YEAR}-{MAX_YEAR}. Exiting.")
    sys.exit(1)

WAIT_MIN, WAIT_MAX        = 2, 10         # per-profile jitter
BURST_SIZE, BURST_PAUSE   = 40, 120       # cool-down after 40 rows
LOAD_WAIT, CF_WAIT, CF_RETRY = 1, 60, 2
RAW_FILE = Path("bnw_bar_raw.csv")

DRILLS = {
    "60 YD":      ("60_time", "60_pct", "60_class_pct", "60_state_pct"),
    "30 YD":      ("30_time", "30_pct", "30_class_pct", "30_state_pct"),
    "Broad Jump": ("broad_ft", "broad_pct", "broad_class_pct", "broad_state_pct"),
    "L-Drill":    ("l_time",  "l_pct",   "l_class_pct",  "l_state_pct"),
    "Med Ball":   ("med_ft",  "med_pct", "med_class_pct","med_state_pct"),
}

# ─── COLUMN ORDER ─────────────────────────────────────────────────────────
BIO_COLS = [
    "city_state", "high_school", "positions", "bat_throw",
    "height", "weight", "summer_team",
]
DRILL_COLS = sum((list(vals) for vals in DRILLS.values()), [])  # flatten
ALL_COLS = (
    ["grad_year", "name"] +
    BIO_COLS +
    ["test_year"] +
    DRILL_COLS +
    ["profile_url", "timestamp"]
)

# ─── small helpers ────────────────────────────────────────────────────────
tidy = lambda s: s.strip().replace("\xa0", "")
slug_to_name = lambda s: s.replace("-", " ").title()
grad_url = lambda y: f"{BASE}/find-player?graduation_year={y}&submit=Submit"
looks_like_cf = lambda h: ("Attention Required!" in h) or ('cf-error-details' in h)
log = lambda m: print(time.strftime("%H:%M:%S"), m, flush=True)

def to_percent(txt:str): return txt.strip().replace("%","").replace("< ","<")

def parse_stat(div):
    label = div.find("h4").get_text(strip=True)
    if label not in DRILLS: return {}
    score = tidy(div.select_one("div.stat-value").text)
    ranks = {rp["data-type"]: to_percent(tidy(rp.text))
             for rp in div.select("div.rank-percentile")}
    sc, po, pc, ps = DRILLS[label]
    return {sc:score, po:ranks.get("overall",""),
            pc:ranks.get("graduation_year",""), ps:ranks.get("state","")}

def parse_bio(info):
    get = lambda sel: tidy(info.select_one(sel).text) if info.select_one(sel) else ""
    bio = {
        "positions":   get(".player-position"),
        "bat_throw":   get(".bat-throw"),
        "height":      get(".height"),
        "weight":      get(".weight"),
        "summer_team": get(".summer-team"),
    }
    hs = info.select_one(".high-school")
    if hs:
        parts=[tidy(t) for t in hs.stripped_strings]
        bio["high_school"]=parts[0].split(",")[0] if parts else ""
        bio["city_state"]=parts[1] if len(parts)>1 else ""
    else:
        bio["high_school"]=bio["city_state"]=""
    return bio

# ─── scrape profile ───────────────────────────────────────────────────────
async def fetch_html(page,url):
    url_bar=url.split("#")[0]+"#player-bar-year"
    for _ in range(CF_RETRY+1):
        await page.goto(url_bar,timeout=45_000)
        html=await page.content()
        if not looks_like_cf(html): return html
        log(f"   Cloudflare – wait {CF_WAIT}s"); await asyncio.sleep(CF_WAIT)
    return html

async def scrape(page,url):
    soup=BeautifulSoup(await fetch_html(page,url),"html.parser")
    if not soup.select_one("div.player-stats"): return []

    slug=urlparse(url).path.split("/")[-1]
    name=slug_to_name(slug)
    sel=soup.select_one("#player-bar-year select.purei-bar-filter-select")
    grad=sel.get("data_graduation_year","0000") if sel else "0000"
    bio=parse_bio(soup.select_one("div.player-info-wrapper") or BeautifulSoup("", "html.parser"))

    rows=[]
    for grp in soup.select("div[id^='player-bar-'] div.stat-group"):
        m=re.search(r"purei-bar-data-(\d{4})"," ".join(grp.get("class",[])))
        if not m: continue
        yr=m.group(1)
        data={"grad_year":grad,"name":name,"test_year":yr}|bio
        for it in grp.select("div.stat-item"): data.update(parse_stat(it))
        for col in DRILL_COLS: data.setdefault(col,"")
        data.update(profile_url=url,timestamp=datetime.utcnow().isoformat(timespec="seconds"))
        rows.append(data)

    if not rows:
        blank={"grad_year":grad,"name":name,"test_year":""}|bio
        for col in DRILL_COLS: blank[col]=""
        blank.update(profile_url=url,timestamp=datetime.utcnow().isoformat(timespec="seconds"))
        rows.append(blank)
    return rows

# ─── collect links per year (same as before) ──────────────────────────────
async def collect_year(page,year):
    log(f"-- Collecting {year} list --")
    await page.goto(grad_url(year),timeout=45_000)
    urls,prev=set(),0
    while True:
        for a in await page.locator("a[href^='/profiles/']").element_handles():
            href=await a.get_attribute("href")
            if href: urls.add(BASE+href)
        btn=page.locator("button:has-text('Load More')")
        if not await btn.count(): break
        if not (await btn.first.is_visible() and await btn.first.is_enabled()): break
        try: await btn.first.click(timeout=5000)
        except (PwTimeout, asyncio.CancelledError): break
        await asyncio.sleep(LOAD_WAIT)
        if len(urls)==prev: break
        prev=len(urls)
    log(f"Collected {len(urls):,} links for {year}")
    return urls

# ─── main ─────────────────────────────────────────────────────────────────
async def main():
    seen=set()
    if RAW_FILE.exists():
        try:
            tmp=pd.read_csv(RAW_FILE,usecols=["name","test_year"])
            seen.update(zip(tmp["name"], tmp["test_year"].fillna("")))
            log(f"Loaded {len(seen):,} existing name-year pairs.")
        except Exception as e:
            log(f"Couldn't read existing CSV ({e})")

    async with async_playwright() as pw:
        browser=await pw.chromium.launch(headless=True)
        page=await browser.new_page()

        links=set()
        for y in YEARS: links.update(await collect_year(page,y))
        log(f"Total unique links: {len(links):,}")

        ok=0
        for idx,url in enumerate(links,1):
            log(f"[{idx}/{len(links)}] {url}")
            for row in await scrape(page,url):
                key=(row["name"], str(row["test_year"]))
                if key in seen: continue
                seen.add(key); ok+=1
                for col in ALL_COLS: row.setdefault(col,"")
                first=not RAW_FILE.exists()
                with open(RAW_FILE,"a",newline="",encoding="utf-8",buffering=1) as f:
                    pd.DataFrame([row], columns=ALL_COLS).to_csv(f,index=False,header=first)
                    f.flush(); os.fsync(f.fileno())
                    if first: log(f"CSV created at {RAW_FILE.resolve()}")
                log(f"   wrote {row['name']} ({row['test_year'] or 'no BAR'})")

            # pacing
            if ok and ok% BURST_SIZE==0:
                log(f"   Burst {BURST_SIZE} – cooling {BURST_PAUSE}s")
                await asyncio.sleep(BURST_PAUSE)
            else:
                await asyncio.sleep(random.uniform(WAIT_MIN,WAIT_MAX))

        await browser.close(); log("Browser closed.")

    if not RAW_FILE.exists():
        log("No rows saved."); return

    # recompute percentiles
    df=pd.read_csv(RAW_FILE)
    idx=list(range(25,100,5))+[99]; grid={}
    for d,(col,*_) in DRILLS.items():
        ser=pd.to_numeric(df[col].astype(str).str.replace("'",".").str.replace('"',''),
                          errors="coerce").dropna()
        if not ser.empty:
            grid[d.replace(" ","_")]=np.percentile(ser,idx).round(2)
    pd.DataFrame(grid,index=idx).rename_axis("Percentile")\
      .to_csv("bnw_bar_percentiles.csv")
    log("Percentiles updated")
    log("bnw_bar_percentiles.csv saved")
    log("=== Finished ===")

if __name__=="__main__":
    asyncio.run(main())

