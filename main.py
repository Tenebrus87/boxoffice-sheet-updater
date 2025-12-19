import os
import json
import time
import datetime as dt
from typing import Optional, List

import numpy as np
import pandas as pd
import requests
import gspread
from google.oauth2.service_account import Credentials
from io import StringIO

# -----------------
# Config (ENV)
# -----------------
SHEET_ID = os.environ["SHEET_ID"]
YEAR = int(os.getenv("YEAR", "2025"))

RAW_TAB = os.getenv("RAW_TAB", "raw")
LEADER_TAB = os.getenv("LEADER_TAB", "leaderboard")

REBUILD = os.getenv("REBUILD", "0") == "1"
APPEND_BATCH_SIZE = int(os.getenv("APPEND_BATCH_SIZE", "500"))

# polite scraping defaults
REQUEST_SLEEP_SECONDS = float(os.getenv("REQUEST_SLEEP_SECONDS", "0.6"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "4"))
TIMEOUT_SECONDS = int(os.getenv("TIMEOUT_SECONDS", "30"))

BASE_URL = "https://www.boxofficemojo.com/date/{date}/"

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}


# -----------------
# Google Sheets helpers
# -----------------
def gs_client():
    info = json.loads(os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"])
    creds = Credentials.from_service_account_info(info, scopes=SCOPES)
    return gspread.authorize(creds)


def ensure_headers(ws):
    if ws.acell("A1").value:
        return
    ws.update([["date", "title", "revenue", "theaters", "distributor"]], range_name="A1:E1")


def get_max_date(ws) -> Optional[dt.date]:
    col_a = ws.col_values(1)
    if len(col_a) <= 1:
        return None
    last = col_a[-1]
    try:
        return dt.datetime.strptime(last, "%Y-%m-%d").date()
    except Exception:
        return None


def append_rows_batched(ws, rows: List[List], batch_size: int = 500) -> int:
    if not rows:
        return 0
    added = 0
    for i in range(0, len(rows), batch_size):
        chunk = rows[i:i + batch_size]
        ws.append_rows(chunk, value_input_option="USER_ENTERED")
        added += len(chunk)
    return added


# -----------------
# BoxOfficeMojo scraping
# -----------------
def _parse_money(s) -> int:
    if pd.isna(s):
        return 0
    s = str(s).strip()
    if not s or s in {"-", "N/A"}:
        return 0
    # remove $ and commas
    s = s.replace("$", "").replace(",", "")
    # some values might contain footnotes or weird chars; coerce
    try:
        return int(float(s))
    except Exception:
        return 0


def _parse_int(s):
    if pd.isna(s):
        return ""
    s = str(s).strip()
    if not s or s in {"-", "N/A"}:
        return ""
    s = s.replace(",", "")
    try:
        return int(s)
    except Exception:
        return ""


def fetch_daily_table(date_str: str) -> pd.DataFrame:
    """Fetch BOM daily table for a given date (YYYY-MM-DD) and return normalized df."""
    url = BASE_URL.format(date=date_str)
    last_err = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(url, headers=HEADERS, timeout=TIMEOUT_SECONDS)
            if r.status_code != 200:
                raise RuntimeError(f"HTTP {r.status_code}")

            # BOM pages usually contain at least one HTML table we can parse
            tables = pd.read_html(StringIO(r.text))
            if not tables:
                raise RuntimeError("No tables found")

            # Pick the table that has 'Release' and 'Daily'
            df = None
            for t in tables:
                cols = [str(c).strip().lower() for c in t.columns]
                if "release" in cols and "daily" in cols:
                    df = t.copy()
                    break
            if df is None:
                raise RuntimeError("Expected table with Release/Daily not found")

            # Normalize columns
            df.columns = [str(c).strip().lower() for c in df.columns]

            # Keep only what we need
            keep = {}
            for col in df.columns:
                if col == "release":
                    keep[col] = "title"
                elif col == "daily":
                    keep[col] = "revenue"
                elif col == "theaters":
                    keep[col] = "theaters"
                elif col == "distributor":
                    keep[col] = "distributor"

            df = df[list(keep.keys())].rename(columns=keep)

            # Clean values
            df["date"] = date_str
            df["revenue"] = df["revenue"].apply(_parse_money)
            if "theaters" in df.columns:
                df["theaters"] = df["theaters"].apply(_parse_int)
            else:
                df["theaters"] = ""

            if "distributor" not in df.columns:
                df["distributor"] = ""

            # drop blanks
            df["title"] = df["title"].astype(str).str.strip()
            df = df[df["title"] != ""].copy()

            # polite sleep
            time.sleep(REQUEST_SLEEP_SECONDS)
            return df[["date", "title", "revenue", "theaters", "distributor"]]

        except Exception as e:
            last_err = e
            # exponential-ish backoff
            time.sleep(min(8, 0.7 * attempt))

    raise RuntimeError(f"Failed to fetch {date_str}: {last_err}")


def date_range(start: dt.date, end: dt.date):
    d = start
    while d <= end:
        yield d
        d += dt.timedelta(days=1)


def scrape_year(start_date: dt.date, end_date: dt.date) -> pd.DataFrame:
    frames = []
    for d in date_range(start_date, end_date):
        ds = d.strftime("%Y-%m-%d")
        day_df = fetch_daily_table(ds)
        frames.append(day_df)
    if not frames:
        return pd.DataFrame(columns=["date", "title", "revenue", "theaters", "distributor"])
    return pd.concat(frames, ignore_index=True)


# -----------------
# Leaderboard writing (tie-break alphabetic)
# -----------------
def write_leaderboard(sh, df_year: pd.DataFrame, year: int):
    ws = sh.worksheet(LEADER_TAB)
    ws.clear()
    ws.update("A1", [[f"Leaderboard {year} (calendar daily sum; tie-break: alphabetic)"]])

    if df_year.empty:
        ws.update("A3", [["No data yet."]])
        return

    totals = (
        df_year.groupby("title", as_index=False)["revenue"].sum()
        .sort_values(["revenue", "title"], ascending=[False, True])
    )
    winner = totals.iloc[0]

    ws.update("A3", [["Winner (current):", winner["title"], float(winner["revenue"])]])

    top50 = totals.head(50).copy()
    top50.insert(0, "rank", range(1, len(top50) + 1))

    ws.update("A5", [["Rank", "Title", "Revenue"]])
    ws.update(
        "A6",
        top50[["rank", "title", "revenue"]].values.tolist(),
        value_input_option="USER_ENTERED",
    )


def main():
    gc = gs_client()
    sh = gc.open_by_key(SHEET_ID)

    raw = sh.worksheet(RAW_TAB)

    if REBUILD:
        raw.clear()

    ensure_headers(raw)

    # Determine scrape window
    year_start = dt.date(YEAR, 1, 1)
    year_end = dt.date(YEAR, 12, 31)

    today = dt.date.today()
    # don't scrape future dates
    effective_end = min(year_end, today)

    max_date = None if REBUILD else get_max_date(raw)
    if max_date:
        start = max(max_date + dt.timedelta(days=1), year_start)
    else:
        start = year_start

    if start > effective_end:
        print(f"Nothing to do. YEAR={YEAR} start={start} > end={effective_end}")
        return

    print(f"Scraping YEAR={YEAR} from {start} to {effective_end} into tabs {RAW_TAB}/{LEADER_TAB}")

    df_year = scrape_year(start, effective_end)

    # Append rows
    rows = df_year.values.tolist()
    added = append_rows_batched(raw, rows, APPEND_BATCH_SIZE)

    # For leaderboard, we want full-year totals so far.
    # Easiest: re-read raw tab (only columns we need) once.
    # This is fine at ~tens of thousands of rows.
    raw_vals = raw.get_all_values()
    if len(raw_vals) <= 1:
        df_all = df_year
    else:
        headers = raw_vals[0]
        data = raw_vals[1:]
        df_all = pd.DataFrame(data, columns=headers)
        df_all["revenue"] = pd.to_numeric(df_all["revenue"], errors="coerce").fillna(0).astype(int)

    write_leaderboard(sh, df_all[["title", "revenue"]], YEAR)

    # simple log
    dates = df_year["date"]
    print(f"Added {added} rows. New date range appended: {dates.min()}..{dates.max()}")

if __name__ == "__main__":
    main()
