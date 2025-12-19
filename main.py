import os
import json
import datetime as dt

import numpy as np
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials

# --- Config ---
DATA_URL = os.getenv(
    "DATA_URL",
    "https://github.com/tjwaterman99/boxofficemojo-scraper/releases/latest/download/revenues_per_day.csv.gz",
)
SHEET_ID = os.environ["SHEET_ID"]
YEAR = int(os.getenv("YEAR", "2026"))

RAW_TAB = os.getenv("RAW_TAB", "raw")
LEADER_TAB = os.getenv("LEADER_TAB", "leaderboard")

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


def gs_client():
    """Authorize gspread using a Service Account JSON stored in env var."""
    info = json.loads(os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"])
    creds = Credentials.from_service_account_info(info, scopes=SCOPES)
    return gspread.authorize(creds)


def download_df() -> pd.DataFrame:
    """Download and normalize the daily revenues dataset, filter to YEAR."""
    df = pd.read_csv(DATA_URL, compression="gzip", parse_dates=["date"])
    df.columns = [c.strip().lower() for c in df.columns]

    for col in ["date", "title", "revenue"]:
        if col not in df.columns:
            raise ValueError(f"Missing expected column: {col}")

    # Normalize revenue to numeric
    if df["revenue"].dtype == "object":
        df["revenue"] = (
            df["revenue"]
            .astype(str)
            .str.replace("$", "", regex=False)
            .str.replace(",", "", regex=False)
            .str.strip()
        )

    df["revenue"] = pd.to_numeric(df["revenue"], errors="coerce")

    # Optional columns
    if "theaters" in df.columns:
        df["theaters"] = pd.to_numeric(df["theaters"], errors="coerce")
    if "distributor" not in df.columns:
        df["distributor"] = ""

    # Filter to the calendar year requested
    df = df[df["date"].dt.year == YEAR].copy()

    # Replace invalid floats with safe values
    df.replace([np.inf, -np.inf], np.nan, inplace=True)
    df["revenue"] = df["revenue"].fillna(0)

    if "theaters" in df.columns:
        df["theaters"] = df["theaters"].fillna("")

    # Ensure types
    df["title"] = df["title"].astype(str)
    df["distributor"] = df["distributor"].astype(str)

    return df


def ensure_headers(ws):
    """Create headers if sheet is empty."""
    if ws.acell("A1").value:
        return
    headers = ["date", "title", "revenue", "theaters", "distributor"]
    ws.update("A1:E1", [headers])


def get_max_date(ws) -> dt.date | None:
    """Read the last date in column A to know what we've already appended."""
    col_a = ws.col_values(1)
    if len(col_a) <= 1:
        return None
    last = col_a[-1]
    try:
        return dt.datetime.strptime(last, "%Y-%m-%d").date()
    except Exception:
        return None


def build_rows(df_new: pd.DataFrame) -> list[list]:
    """Build JSON-safe rows for gspread (no NaN/Inf)."""
    # Guarantee required columns exist
    for c in ["theaters", "distributor"]:
        if c not in df_new.columns:
            df_new[c] = ""

    # Clean invalids
    df_new = df_new.replace([np.inf, -np.inf], np.nan)

    # Revenue: missing -> 0
    df_new["revenue"] = pd.to_numeric(df_new["revenue"], errors="coerce").fillna(0)

    # Theaters: missing -> ""
    df_new["theaters"] = pd.to_numeric(df_new["theaters"], errors="coerce")
    df_new["theaters"] = df_new["theaters"].replace([np.inf, -np.inf], np.nan).fillna("")

    rows = []
    for _, r in df_new.iterrows():
        revenue = r["revenue"]
        if pd.isna(revenue) or revenue in [float("inf"), float("-inf")]:
            revenue = 0

        theaters = r["theaters"]
        if pd.isna(theaters) or theaters in [float("inf"), float("-inf")]:
            theaters = ""

        # theaters to int if it's numeric, else keep empty/string
        if isinstance(theaters, (int, float)) and theaters != "":
            try:
                theaters = int(theaters)
            except Exception:
                theaters = ""

        rows.append(
            [
                r["date"].strftime("%Y-%m-%d"),
                str(r["title"]),
                float(revenue),  # JSON-safe now
                theaters,
                str(r.get("distributor", "")),
            ]
        )
    return rows


def write_leaderboard(sh, df_year: pd.DataFrame):
    """Write a simple top list and current winner with tie-break alphabetically."""
    totals = (
        df_year.groupby("title", as_index=False)["revenue"].sum()
        .sort_values(["revenue", "title"], ascending=[False, True])
    )

    if totals.empty:
        # Nothing to write yet
        ws = sh.worksheet(LEADER_TAB)
        ws.clear()
        ws.update("A1", [[f"Leaderboard {YEAR} (no data yet)"]])
        return

    winner = totals.iloc[0]

    top50 = totals.head(50).copy()
    top50.insert(0, "rank", range(1, len(top50) + 1))

    ws = sh.worksheet(LEADER_TAB)
    ws.clear()
    ws.update("A1", [[f"Leaderboard {YEAR} (calendar revenue, tie-break: alphabetic)"]])
    ws.update("A3", [["Winner (current):", winner["title"], float(winner["revenue"])]])

    ws.update("A5", [["Rank", "Title", "Revenue"]])
    ws.update("A6", top50[["rank", "title", "revenue"]].values.tolist(), value_input_option="USER_ENTERED")


def main():
    gc = gs_client()
    sh = gc.open_by_key(SHEET_ID)

    raw = sh.worksheet(RAW_TAB)
    ensure_headers(raw)

    max_date = get_max_date(raw)

    df = download_df()

    # If the sheet already has data, append only rows newer than the last date in column A.
    if max_date:
        df_new = df[df["date"].dt.date > max_date].copy()
    else:
        df_new = df.copy()

    # Sort for stable appends
    df_new = df_new.sort_values(["date", "title"])

    # Append new rows
    rows = build_rows(df_new)
    if rows:
        raw.append_rows(rows, value_input_option="USER_ENTERED")

    # Recompute leaderboard from full-year subset
    write_leaderboard(sh, df)

    print(
        f"YEAR={YEAR} max_date_in_sheet={max_date} new_rows_added={len(rows)} total_rows_year={len(df)}"
    )


if __name__ == "__main__":
    main()
