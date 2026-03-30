#!/usr/bin/env python3
"""
PM India Domestic Visits Scraper
Install:  pip install requests beautifulsoup4 pandas
Run:      python domestic_scraper.py

OUTPUT: pm_domestic_visits.csv with exactly 3 columns:
  period  — "Mar 28, 2026 - Mar 28, 2026"
  year    — 2026
  state   — Uttar Pradesh   (clean state name ONLY, one per row)
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
from pathlib import Path
import re
import time

OUTPUT_CSV  = Path("pm_domestic_visits.csv")
TOTAL_PAGES = 81

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Referer":         "https://www.pmindia.gov.in/en/",
}

# Matches date ranges like: (Mar 28, 2026 - Mar 28, 2026)
DATE_RE = re.compile(
    r"([A-Z][a-z]{2}\s+\d{1,2},\s+\d{4})\s*[-–]\s*([A-Z][a-z]{2}\s+\d{1,2},\s+\d{4})"
)

# Split states on & or "and" — NOT commas (commas appear in dates)
STATE_SEP = re.compile(r"\s*&\s*|\s+and\s+", re.IGNORECASE)

# List of all known Indian states/UTs for matching
KNOWN_STATES = [
    "Andhra Pradesh", "Arunachal Pradesh", "Assam", "Bihar", "Chhattisgarh",
    "Goa", "Gujarat", "Haryana", "Himachal Pradesh", "Jharkhand", "Karnataka",
    "Kerala", "Madhya Pradesh", "Maharashtra", "Manipur", "Meghalaya", "Mizoram",
    "Nagaland", "Odisha", "Punjab", "Rajasthan", "Sikkim", "Tamil Nadu",
    "Telangana", "Tripura", "Uttar Pradesh", "Uttarakhand", "West Bengal",
    "Delhi", "Jammu and Kashmir", "Ladakh", "Puducherry", "Pondicherry",
    "Chandigarh", "Andaman and Nicobar", "Lakshadweep", "Keralam",
    "Dadra and Nagar Haveli", "Daman and Diu",
]
# Lowercase lookup for fast matching
STATES_LOWER = {s.lower(): s for s in KNOWN_STATES}


def clean_state_name(raw: str) -> str:
    """
    Clean a raw state string — remove anything that is not letters/spaces.
    Then try to match against known state names. Return clean title-case name.

    Examples:
      "Uttar Pradesh(Feb 22"  → "Uttar Pradesh"
      "2026 - Feb 22"         → ""  (will be filtered out)
      "Assam"                 → "Assam"
    """
    # Remove anything from first digit or bracket onward
    name = re.split(r"[\(\[0-9]", raw)[0].strip()
    # Remove trailing punctuation
    name = name.strip(".,;:-").strip()
    # Must be only letters and spaces
    if not re.match(r"^[A-Za-z\s]+$", name):
        return ""
    # Check against known states (case-insensitive)
    lower = name.lower().strip()
    if lower in STATES_LOWER:
        return STATES_LOWER[lower]
    # Partial match — if name is contained in a known state or vice versa
    for key, val in STATES_LOWER.items():
        if lower == key or lower in key or key in lower:
            return val
    # Return cleaned title-case name anyway
    return name.title().strip() if len(name) > 2 else ""


def get_states_from_title(title: str) -> list:
    """
    Extract clean state names from a visit title.
    'PM's visit to Assam & West Bengal' → ['Assam', 'West Bengal']
    'PM's visit to Uttar Pradesh'       → ['Uttar Pradesh']
    """
    # Get destination portion
    m = re.search(r"visit to (.+?)(?:\s*\[|$)", title, re.IGNORECASE)
    if not m:
        return []
    dest = re.sub(r"\[.*?\]", "", m.group(1)).strip()

    # Split on & or "and"
    parts = STATE_SEP.split(dest)
    states = []
    for part in parts:
        cleaned = clean_state_name(part.strip())
        if cleaned:
            states.append(cleaned)

    return states if states else []


def get_soup(url: str) -> BeautifulSoup:
    s = requests.Session()
    try:
        s.get("https://www.pmindia.gov.in/en/", headers=HEADERS, timeout=30)
    except Exception:
        pass
    time.sleep(1)
    r = s.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    return BeautifulSoup(r.text, "html.parser")


def parse_page(soup: BeautifulSoup) -> list:
    """
    Parse visits from one page. Returns list of dicts with:
      period, year, state
    """
    rows = []
    seen = set()

    body = (
        soup.find("div", class_=re.compile(r"view-content|region-content", re.I))
        or soup.find("main")
        or soup.body
    )
    if not body:
        return rows

    for a in body.find_all("a", href=True):
        title = a.get_text(strip=True)
        if not title or "visit" not in title.lower() or len(title) < 15:
            continue

        # Remove bracket annotations
        clean_title = re.sub(r"\s*\[.*?\]", "", title).strip()
        if clean_title in seen:
            continue
        seen.add(clean_title)

        # Find the date in the surrounding container
        container = a.parent
        period = ""
        for _ in range(6):
            if container is None:
                break
            m = DATE_RE.search(container.get_text(" "))
            if m:
                period = f"{m.group(1).strip()} - {m.group(2).strip()}"
                break
            container = container.parent

        if not period:
            continue  # no date = nav link, skip

        # Extract year from period
        yr_match = re.search(r"(\d{4})", period)
        year = yr_match.group(1) if yr_match else "Unknown"

        # Get clean state names — ONE ROW PER STATE
        states = get_states_from_title(clean_title)
        if not states:
            # Fallback: use destination as-is if no known state matched
            m2 = re.search(r"visit to (.+?)(?:\s*\[|$)", clean_title, re.IGNORECASE)
            if m2:
                dest = re.sub(r"\[.*?\]", "", m2.group(1)).strip()
                # Only add if it looks like clean text (no numbers/dates)
                if re.match(r"^[A-Za-z\s&,]+$", dest):
                    states = [dest.strip()]

        for state in states:
            rows.append({
                "period": period,
                "year":   year,
                "state":  state,
            })

    return rows


def page_url(n: int) -> str:
    return f"https://www.pmindia.gov.in/en/pm-visits/page/{n}/?visittype=domestic_visit"


def scrape_all() -> pd.DataFrame:
    all_rows = []
    for n in range(1, TOTAL_PAGES + 1):
        url = page_url(n)
        print(f"  Page {n:>2}/{TOTAL_PAGES}  →  ", end="", flush=True)
        try:
            soup = get_soup(url)
            rows = parse_page(soup)
            all_rows.extend(rows)
            print(f"{len(rows)} rows")
        except Exception as e:
            print(f"FAILED: {e}")
        time.sleep(1.2)
    df = pd.DataFrame(all_rows)
    print(f"\n  Grand total: {len(df)} rows")
    return df


def scrape_latest() -> pd.DataFrame:
    url  = page_url(1)
    soup = get_soup(url)
    rows = parse_page(soup)
    print(f"  Page 1 → {len(rows)} rows")
    return pd.DataFrame(rows)


def sort_newest(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["_yr"] = df["year"].apply(lambda y: int(y) if str(y).isdigit() else 0)
    df["_dt"] = df["period"].str.extract(r"^([A-Z][a-z]{2}\s+\d{1,2},\s+\d{4})")
    df["_dt"] = pd.to_datetime(df["_dt"], format="%b %d, %Y", errors="coerce")
    df.sort_values(["_yr", "_dt"], ascending=[False, False],
                   inplace=True, na_position="last")
    df.drop(columns=["_yr", "_dt"], inplace=True)
    df.reset_index(drop=True, inplace=True)
    return df


def run(force_full: bool = False):
    # Delete CSV if it has wrong columns
    if OUTPUT_CSV.exists():
        try:
            check = pd.read_csv(OUTPUT_CSV, nrows=1)
            if set(check.columns) != {"period", "year", "state"}:
                print("⚠  Old CSV has wrong columns — deleting and re-scraping.")
                OUTPUT_CSV.unlink()
        except Exception:
            OUTPUT_CSV.unlink()

    first_run = not OUTPUT_CSV.exists() or force_full

    if first_run:
        print(f"\n=== FULL SCRAPE — all {TOTAL_PAGES} pages ===\n")
        df = scrape_all()
        if df.empty:
            print("\n⚠  Nothing scraped. Try again in a few minutes.")
            return
        df.drop_duplicates(subset=["period", "state"], keep="last", inplace=True)
        df = sort_newest(df)
        df.to_csv(OUTPUT_CSV, index=False)
        print(f"\n✅  Saved {len(df)} rows → {OUTPUT_CSV.resolve()}")

    else:
        print("\n=== WEEKLY UPDATE — page 1 ===\n")
        existing = pd.read_csv(OUTPUT_CSV, dtype=str).fillna("")
        latest   = scrape_latest()
        if latest.empty:
            print("\n✅  Nothing new.")
            return

        old_keys = set(zip(
            existing["period"].str.strip(),
            existing["state"].str.strip(),
        ))
        latest["_k"] = list(zip(
            latest["period"].str.strip(),
            latest["state"].str.strip(),
        ))
        new_rows = latest[~latest["_k"].isin(old_keys)].drop(columns=["_k"])

        if new_rows.empty:
            print("\n✅  No new entries.")
        else:
            combined = pd.concat([new_rows, existing], ignore_index=True)
            combined = sort_newest(combined)
            combined.to_csv(OUTPUT_CSV, index=False)
            print(f"\n✅  Added {len(new_rows)} rows. Total: {len(combined)}")

    print("\n--- Most recent 5 ---")
    print(pd.read_csv(OUTPUT_CSV, dtype=str).head(5).to_string())


if __name__ == "__main__":
    run()   # ← update mode: checks page 1 only
