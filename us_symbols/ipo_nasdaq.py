from __future__ import annotations

import datetime as dt
from pathlib import Path
from typing import Dict, List

import pandas as pd
import requests

from .utils import DEFAULT_CACHE_DIR, ensure_dir, read_json_cached, throttle


HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json, text/plain, */*",
    "Origin": "https://www.nasdaq.com",
    "Referer": "https://www.nasdaq.com/",
}


def _fetch_month_json(year: int, month: int) -> dict:
    url = f"https://api.nasdaq.com/api/ipo/calendar?date={year}-{month:02d}"
    r = requests.get(url, headers=HEADERS, timeout=20)
    r.raise_for_status()
    return r.json()


def fetch_nasdaq_ipo_month(year: int, month: int, refresh: bool = False, cache_dir: Path = DEFAULT_CACHE_DIR) -> List[Dict]:
    cache_file = cache_dir / "nasdaq_ipo" / f"{year}-{month:02d}.json"

    def fetch():
        return _fetch_month_json(year, month)

    j = read_json_cached("", cache_file, refresh=refresh, fetch_fn=fetch)
    rows = []
    try:
        priced = j.get("data", {}).get("priced", {}).get("rows", []) or []
        for row in priced:
            sym = (row.get("symbol") or row.get("proposedTickerSymbol") or "").strip().upper()
            date = (row.get("priced") or row.get("date") or "").strip()
            comp = (row.get("companyName") or "").strip()
            if sym and date:
                rows.append({"Symbol": sym, "IPODate": date, "Company": comp})
    except Exception:
        pass
    return rows


def fetch_nasdaq_ipo_range(
    start_year: int = 1998,
    end_year: int | None = None,
    end_month: int | None = None,
    refresh: bool = False,
    cache_dir: Path = DEFAULT_CACHE_DIR,
) -> pd.DataFrame:
    today = dt.date.today()
    y = end_year or today.year
    m = end_month or today.month
    all_rows: List[Dict] = []
    for year in range(start_year, y + 1):
        for month in range(1, 13):
            if year == y and month > m:
                break
            rows = fetch_nasdaq_ipo_month(year, month, refresh=refresh, cache_dir=cache_dir)
            all_rows.extend(rows)
            throttle(0.5)
    df = pd.DataFrame(all_rows)
    if df.empty:
        return pd.DataFrame(columns=["Symbol", "IPODate", "Company"])  # stable schema
    df["IPODate"] = pd.to_datetime(df["IPODate"], errors="coerce").dt.date
    df = df.dropna(subset=["IPODate"]).drop_duplicates(["Symbol", "IPODate"])  # de-dupe
    return df
