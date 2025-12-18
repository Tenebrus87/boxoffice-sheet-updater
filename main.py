import os, json, datetime as dt
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials

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
    info = json.loads(os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"])
    creds = Credentials.from_service_account_info(info, scopes=SCOPES)
    return gspread.authorize(creds)

def download_df():
    df = pd.read_csv(DATA_URL, compression="gzip", parse_dates=["date"])
    df.columns = [c.strip().lower() for c in df.columns]
    df = df[df["date"].dt.year == YEAR].copy()

    # revenue numeric
    if df["revenue"].dtype == "object":
        df["revenue"] = (
            df["revenue"].astype(str)
            .str.replace("$", "", regex=False)
            .str.replace(",", "", regex=False)
        )
    df["revenue"] = pd.to_numeric(df["revenue"], errors="coerce").fillna(0)
    return df

def ensure_headers(ws):
    if ws.acell("A1").value:
        return
    ws.update("A1:E1", [["date", "title", "revenue", "theaters", "distributor"]])

def get_max_date(ws):
    col_a = ws.col_values(1)
    if len(col_a) <= 1:
        return None
    try:
        return dt.datetime.strptime(col_a[-1], "%Y-%m-%d").date()
    except Exception:
        return None

def write_leaderboard(sh, df_year):
    totals = (df_year.groupby("title", as_index=False)["revenue"].sum()
              .sort_values(["revenue", "title"], ascending=[False, True]))
    ws = sh.worksheet(LEADER_TAB)
    ws.clear()
    if len(totals) == 0:
        ws.update("A1", [[f"Leaderboard {YEAR} (no data yet)"]])
        return

    winner = totals.iloc[0]
    top50 = totals.head(50).copy()
    top50.insert(0, "rank", range(1, len(top50) + 1))

    ws.update("A1", [[f"Leaderboard {YEAR} (calendar revenue, tie-break: alphabetic)"]])
    ws.update("A3", [["Winner (current):", winner["title"], float(winner["revenue"])]] )
    ws.update("A5", [["Rank", "Title", "Revenue"]])
    ws.update("A6", top50[["rank","title","revenue"]].values.tolist(), value_input_option="USER_ENTERED")

def main():
    gc = gs_client()
    sh = gc.open_by_key(SHEET_ID)

    raw = sh.worksheet(RAW_TAB)
    ensure_headers(raw)
    max_date = get_max_date(raw)

    df = download_df()
    if max_date:
        df_new = df[df["date"].dt.date > max_date].copy()
    else:
        df_new = df.copy()

    # Ensure optional cols exist
    for c in ["theaters", "distributor"]:
        if c not in df_new.columns:
            df_new[c] = ""

    df_new = df_new.sort_values(["date", "title"])
    rows = [[
        r["date"].strftime("%Y-%m-%d"),
        str(r["title"]),
        float(r["revenue"]),
        r.get("theaters",""),
        r.get("distributor",""),
    ] for _, r in df_new.iterrows()]

    if rows:
        raw.append_rows(rows, value_input_option="USER_ENTERED")

    write_leaderboard(sh, df)
    print(f"YEAR={YEAR} max_date={max_date} new_rows={len(rows)} total_rows_year={len(df)}")

if __name__ == "__main__":
    main()
