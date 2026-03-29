# India PM Visits Data

A dataset tracking all foreign visits made by the Prime Minister of India,
automatically updated on the 1st of every month.

---

## What It Is

This repository collects and maintains a structured CSV of every foreign visit
made by India's Prime Minister, sourced directly from the official
[PMIndia website](https://www.pmindia.gov.in/en/). It covers both the full
historical archive and ongoing current visits, updated automatically each month
via GitHub Actions.

---

## Method

Data is scraped from two official PMIndia pages:

- **Historical archive**: `pmindia.gov.in/en/previous-foreign-visits/`
- **Current visits**: `pmindia.gov.in/en/details-of-foreigndomestic-visits/`

**First run**: pulls all historical + current data and merges them.  
**Monthly updates**: checks only the current page and appends new rows.

The scraper enriches each row with:
- `year` — extracted from the original date string
- `continent` — derived from country name
- `majority_religion` — majority religion of the visited country
- `multi_country_visit` — flags trips where multiple countries were visited on the same date

Rows are sorted **newest trip first**. Dates are kept exactly as they appear
on the source — never modified.

> ⚠️ **Note on multi-country visits**: Where the source lists multiple countries
> for the same date, each country gets its own row and is flagged
> `multi_country_visit = YES`. Please verify the full itinerary of such trips
> independently using official PMO press releases or MEA records before publishing.

---

## CSV Structure

| Column | Description |
|---|---|
| `serial_no` | Row number from source |
| `country` | Country visited (one per row) |
| `date` | Original date string from source |
| `year` | 4-digit year extracted from date |
| `continent` | Continent of the country |
| `majority_religion` | Majority religion of the country |
| `expense_inr_lakhs` | Chartered flight cost in INR lakhs (archive rows only) |
| `multi_country_visit` | YES if part of a multi-country trip, NO otherwise |
| `source_url` | URL the row was scraped from |

---

## Using the CSV

Download the latest `pm_visits.csv` directly from this repository.
It is ready to open in Excel, Google Sheets, or any data analysis tool.

For journalists and researchers: the `multi_country_visit` column is particularly
useful for identifying foreign tours vs. single-country state visits. Always
cross-reference with official MEA or PMO records before publishing.

---

## Running the Scraper Locally

```bash
pip install requests beautifulsoup4 pandas
python india-pm-modi-foreign-visits.py
