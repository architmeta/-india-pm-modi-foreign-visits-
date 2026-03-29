#!/usr/bin/env python3
"""
PM India Visits Auto-Scraper
=============================
STEP 1 — Install required libraries (run this ONE time in Terminal/Command Prompt):
    pip install requests beautifulsoup4 pandas

STEP 2 — Run the scraper:
    python scraper.py

First time: pulls ALL historical data from both PM India pages → saves pm_visits.csv
Every time after: only checks the current page and adds new rows

SORT ORDER: Newest trip first (most recent date at the top of the CSV).

DATA NOTE — multi_country_visit column:
    YES = this row was split from a single source row listing multiple countries
          for the same date. All those rows share the same original date string.
          This means the PM visited all these countries during one trip.
    NO  = single country was listed for this date.
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
from pathlib import Path
import re
import time

# ─── Output file ──────────────────────────────────────────────────────────────
OUTPUT_CSV   = Path("pm_visits.csv")

# ─── Source URLs ──────────────────────────────────────────────────────────────
URL_CURRENT  = "https://www.pmindia.gov.in/en/details-of-foreigndomestic-visits/"
URL_PREVIOUS = "https://www.pmindia.gov.in/en/previous-foreign-visits/"

# ─── Browser headers to avoid 403 block ──────────────────────────────────────
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Referer": "https://www.pmindia.gov.in/en/",
}

# ─── Country meta continent + majority religion ─────────────────────────
COUNTRY_META = {
    "afghanistan":              ("Asia",           "Islam"),
    "argentina":                ("South America",  "Christianity"),
    "australia":                ("Oceania",        "Christianity"),
    "austria":                  ("Europe",         "Christianity"),
    "bahrain":                  ("Asia",           "Islam"),
    "bangladesh":               ("Asia",           "Islam"),
    "belgium":                  ("Europe",         "Christianity"),
    "bhutan":                   ("Asia",           "Buddhism"),
    "brazil":                   ("South America",  "Christianity"),
    "brunei":                   ("Asia",           "Islam"),
    "cambodia":                 ("Asia",           "Buddhism"),
    "canada":                   ("North America",  "Christianity"),
    "china":                    ("Asia",           "No majority / Buddhism"),
    "colombia":                 ("South America",  "Christianity"),
    "croatia":                  ("Europe",         "Christianity"),
    "cyprus":                   ("Europe",         "Christianity"),
    "czech republic":           ("Europe",         "Christianity"),
    "denmark":                  ("Europe",         "Christianity"),
    "dubai":                    ("Asia",           "Islam"),
    "egypt":                    ("Africa",         "Islam"),
    "ethiopia":                 ("Africa",         "Christianity"),
    "fiji":                     ("Oceania",        "Christianity"),
    "finland":                  ("Europe",         "Christianity"),
    "france":                   ("Europe",         "Christianity"),
    "germany":                  ("Europe",         "Christianity"),
    "ghana":                    ("Africa",         "Christianity"),
    "greece":                   ("Europe",         "Christianity"),
    "guyana":                   ("South America",  "Christianity"),
    "hungary":                  ("Europe",         "Christianity"),
    "indonesia":                ("Asia",           "Islam"),
    "iran":                     ("Asia",           "Islam"),
    "ireland":                  ("Europe",         "Christianity"),
    "israel":                   ("Asia",           "Judaism"),
    "italy":                    ("Europe",         "Christianity"),
    "japan":                    ("Asia",           "Shinto / Buddhism"),
    "jordan":                   ("Asia",           "Islam"),
    "kazakhstan":               ("Asia",           "Islam"),
    "kenya":                    ("Africa",         "Christianity"),
    "kuwait":                   ("Asia",           "Islam"),
    "kyrgyzstan":               ("Asia",           "Islam"),
    "laos":                     ("Asia",           "Buddhism"),
    "malaysia":                 ("Asia",           "Islam"),
    "maldives":                 ("Asia",           "Islam"),
    "mauritius":                ("Africa",         "Hinduism"),
    "mexico":                   ("North America",  "Christianity"),
    "mongolia":                 ("Asia",           "Buddhism"),
    "mozambique":               ("Africa",         "Christianity"),
    "myanmar":                  ("Asia",           "Buddhism"),
    "namibia":                  ("Africa",         "Christianity"),
    "nepal":                    ("Asia",           "Hinduism"),
    "netherlands":              ("Europe",         "Christianity"),
    "new zealand":              ("Oceania",        "Christianity"),
    "nigeria":                  ("Africa",         "Islam / Christianity"),
    "norway":                   ("Europe",         "Christianity"),
    "oman":                     ("Asia",           "Islam"),
    "pakistan":                 ("Asia",           "Islam"),
    "palestine":                ("Asia",           "Islam"),
    "papua new guinea":         ("Oceania",        "Christianity"),
    "philippines":              ("Asia",           "Christianity"),
    "poland":                   ("Europe",         "Christianity"),
    "portugal":                 ("Europe",         "Christianity"),
    "qatar":                    ("Asia",           "Islam"),
    "russia":                   ("Europe",         "Christianity"),
    "rwanda":                   ("Africa",         "Christianity"),
    "samarkand":                ("Asia",           "Islam"),
    "saudi arabia":             ("Asia",           "Islam"),
    "seychelles":               ("Africa",         "Christianity"),
    "singapore":                ("Asia",           "Buddhism"),
    "south africa":             ("Africa",         "Christianity"),
    "south korea":              ("Asia",           "No majority"),
    "spain":                    ("Europe",         "Christianity"),
    "sri lanka":                ("Asia",           "Buddhism"),
    "sweden":                   ("Europe",         "Christianity"),
    "switzerland":              ("Europe",         "Christianity"),
    "tajikistan":               ("Asia",           "Islam"),
    "tanzania":                 ("Africa",         "Christianity / Islam"),
    "thailand":                 ("Asia",           "Buddhism"),
    "trinidad & tobago":        ("North America",  "Christianity"),
    "trinidad and tobago":      ("North America",  "Christianity"),
    "turkey":                   ("Europe / Asia",  "Islam"),
    "turkmenistan":             ("Asia",           "Islam"),
    "uae":                      ("Asia",           "Islam"),
    "uganda":                   ("Africa",         "Christianity"),
    "ukraine":                  ("Europe",         "Christianity"),
    "united arab emirates":     ("Asia",           "Islam"),
    "united kingdom":           ("Europe",         "Christianity"),
    "united states":            ("North America",  "Christianity"),
    "united states of america": ("North America",  "Christianity"),
    "usa":                      ("North America",  "Christianity"),
    "uzbekistan":               ("Asia",           "Islam"),
    "vietnam":                  ("Asia",           "Buddhism"),
    "uk":                       ("Europe",         "Christianity"),
}

COUNTRY_SEPARATORS = re.compile(r"\s*[,/&]\s*|\s+and\s+", re.IGNORECASE)


def split_countries(raw: str) -> list:
    """Split a country cell that may contain multiple countries into a list."""
    parts = COUNTRY_SEPARATORS.split(raw.strip())
    return [p.strip() for p in parts if p.strip()]


def get_country_meta(country: str):
    """Return (Continent, MajorityReligion) for a country name."""
    key = country.strip().lower()
    if key in COUNTRY_META:
        return COUNTRY_META[key]
    for k, v in COUNTRY_META.items():
        if k in key or key in k:
            return v
    return (
        "Unknown — add to COUNTRY_META dict",
        "Unknown — add to COUNTRY_META dict",
    )


def extract_year(date_str: str) -> str:
    """Pull a 4-digit year out of any date string."""
    match = re.search(r"(\d{4})", date_str)
    return match.group(1) if match else "Unknown"


def sort_newest_first(df: pd.DataFrame) -> pd.DataFrame:         # ← CHANGED
    """
    Sort the dataframe so the most recent trips appear first.
    Strategy: extract the latest year found in the date string,
    then sort descending by that year, then by the original serial_no
    descending (higher serial = more recent on the source website).
    This handles messy date formats like '12-15 Jan 2024' gracefully.
    """
    df = df.copy()
    # Extract the last 4-digit year in the date string for sorting
    df["_sort_year"] = df["date"].str.extract(r"(\d{4})").astype(float)  # ← CHANGED
    # serial_no as numeric for tie-breaking                               # ← CHANGED
    df["_sort_serial"] = pd.to_numeric(df["serial_no"], errors="coerce") # ← CHANGED
    df.sort_values(                                                        # ← CHANGED
        by=["_sort_year", "_sort_serial"],                                 # ← CHANGED
        ascending=[False, False],   # newest year first, highest serial first  # ← CHANGED
        inplace=True,                                                      # ← CHANGED
        na_position="last",                                                # ← CHANGED
    )
    df.drop(columns=["_sort_year", "_sort_serial"], inplace=True)        # ← CHANGED
    df.reset_index(drop=True, inplace=True)                              # ← CHANGED
    return df                                                             # ← CHANGED


def fetch_page(url: str) -> BeautifulSoup:
    """Open a session, visit homepage first to get cookies, then fetch target URL."""
    session = requests.Session()
    try:
        session.get("https://www.pmindia.gov.in/en/", headers=HEADERS, timeout=30)
    except Exception:
        pass
    time.sleep(1)
    response = session.get(url, headers=HEADERS, timeout=30)
    response.raise_for_status()
    return BeautifulSoup(response.text, "html.parser")


def parse_visits_table(soup: BeautifulSoup, has_expense_col: bool, source_url: str) -> list:
    """
    Read every table row on the page.
    If the country cell has multiple countries, creates one row per country.
    multi_country_visit = YES flags those rows.
    """
    rows = []
    for tr in soup.find_all("tr"):
        cells = [td.get_text(strip=True) for td in tr.find_all("td")]
        if len(cells) < 3:
            continue
        if not cells[0].isdigit():
            continue

        serial_no    = cells[0]
        raw_country  = cells[1]
        date_str     = cells[2]
        expense      = cells[3] if (has_expense_col and len(cells) > 3) else ""

        country_list = split_countries(raw_country)
        is_multi     = "YES" if len(country_list) > 1 else "NO"

        for country in country_list:
            continent, religion = get_country_meta(country)
            rows.append({
                "serial_no":            serial_no,
                "country":              country,
                "date":                 date_str,
                "year":                 extract_year(date_str),
                "continent":            continent,
                "majority_religion":    religion,
                "expense_inr_lakhs":    expense,
                "multi_country_visit":  is_multi,
                "source_url":           source_url,
            })
    return rows


def scrape_previous_visits() -> pd.DataFrame:
    print(f"  Fetching historical archive: {URL_PREVIOUS}")
    soup = fetch_page(URL_PREVIOUS)
    rows = parse_visits_table(soup, has_expense_col=True, source_url=URL_PREVIOUS)
    df = pd.DataFrame(rows)
    print(f"  → {len(df)} rows found in archive")
    return df


def scrape_current_visits() -> pd.DataFrame:
    print(f"  Fetching current visits page: {URL_CURRENT}")
    soup = fetch_page(URL_CURRENT)
    rows = parse_visits_table(soup, has_expense_col=False, source_url=URL_CURRENT)
    df = pd.DataFrame(rows)
    print(f"  → {len(df)} rows found on current page")
    return df


def print_multi_country_summary(df: pd.DataFrame):
    """Print a journalist note listing all multi-country trips at the end of the run."""
    multi = df[df["multi_country_visit"] == "YES"].copy()
    if multi.empty:
        print("\n  (No multi-country visits detected in this dataset)")
        return

    print("\n" + "=" * 70)
    print("  DATA NOTE — MULTI-COUNTRY VISITS DETECTED")
    print("=" * 70)
    print(
        "  The rows below were listed with multiple countries for the same date.\n"
        "  Each country has its own row in the CSV. The date column reflects\n"
        "  the ORIGINAL date string from the source — it was NOT changed.\n"
        "  Please verify the full itinerary independently using official\n"
        "  PMO press releases or MEA records before publishing.\n"
        "  Column 'multi_country_visit' = YES flags these rows in the CSV.\n"
    )
    grouped = multi.groupby(["serial_no", "date"])["country"].apply(list).reset_index()
    grouped.columns = ["serial_no", "date", "countries_visited"]
    for _, row in grouped.iterrows():
        countries_str = " → ".join(row["countries_visited"])
        print(f"  Serial #{row['serial_no']}  |  Date: {row['date']}")
        print(f"  Countries: {countries_str}\n")
    print("=" * 70)


def run():
    is_first_run = not OUTPUT_CSV.exists()

    if is_first_run:
        print("\n=== FIRST RUN — scraping both historical + current pages ===\n")
        df_previous = scrape_previous_visits()
        time.sleep(2)
        df_current  = scrape_current_visits()

        df_all = pd.concat([df_previous, df_current], ignore_index=True)
        df_all.drop_duplicates(subset=["country", "date"], keep="last", inplace=True)
        df_all = sort_newest_first(df_all)          # ← CHANGED: sort before saving
        df_all.to_csv(OUTPUT_CSV, index=False)
        print(f"\n✅  Saved {len(df_all)} rows → {OUTPUT_CSV.resolve()}")
        print_multi_country_summary(df_all)

    else:
        print("\n=== MONTHLY UPDATE — checking current page for new rows only ===\n")
        df_existing = pd.read_csv(OUTPUT_CSV, dtype=str).fillna("")
        df_current  = scrape_current_visits()

        existing_keys = set(
            zip(df_existing["country"].str.strip(), df_existing["date"].str.strip())
        )
        df_current["_key"] = list(
            zip(df_current["country"].str.strip(), df_current["date"].str.strip())
        )
        df_new_rows = df_current[~df_current["_key"].isin(existing_keys)].drop(
            columns=["_key"]
        )

        if df_new_rows.empty:
            print("\n✅  No new rows found. Your CSV is already up to date.")
        else:
            # New rows go on top, then re-sort the whole file newest first  # ← CHANGED
            df_combined = pd.concat([df_new_rows, df_existing], ignore_index=True)  # ← CHANGED
            df_combined = sort_newest_first(df_combined)                    # ← CHANGED
            df_combined.to_csv(OUTPUT_CSV, index=False)
            print(
                f"\n✅  Added {len(df_new_rows)} new rows. "
                f"Total: {len(df_combined)} rows → {OUTPUT_CSV.resolve()}"
            )
            print_multi_country_summary(df_new_rows)

    print("\n--- First 5 rows in CSV (newest trips) ---")          # ← CHANGED
    print(
        pd.read_csv(OUTPUT_CSV, dtype=str).head(5)[               # ← CHANGED: head not tail
            ["country", "date", "year", "continent",
             "majority_religion", "multi_country_visit"]
        ].to_string()
    )


if __name__ == "__main__":
    run()
