from __future__ import annotations

from pathlib import Path
from typing import Dict, Iterable, List, Tuple, Optional

import pandas as pd
import yfinance as yf

from .normalize import to_yahoo_symbol
from .utils import throttle
from tqdm import tqdm


def earliest_vendor_dates(
    tickers: Iterable[str],
    batch_size: int = 50,
    pause: float = 1.0,
    max_retries: int = 3,
    verbose: bool = False,
) -> pd.DataFrame:
    """Fetch earliest available monthly bar date per ticker from Yahoo.

    Returns DataFrame with columns: Symbol, EarliestVendorDate
    """
    tickers = list(dict.fromkeys([t.strip().upper() for t in tickers if t and isinstance(t, str)]))
    yahoo_map = {t: to_yahoo_symbol(t) for t in tickers}

    results: Dict[str, pd.Timestamp] = {}
    # Process in batches
    pbar = tqdm(total=len(tickers), desc="Yahoo earliest dates", disable=not verbose)
    for i in range(0, len(tickers), batch_size):
        batch = tickers[i : i + batch_size]
        ybatch = [yahoo_map[t] for t in batch]
        # Batch fetch with retries on rate-limit
        df = None
        attempt = 0
        while attempt <= max_retries and df is None:
            try:
                df = yf.download(
                    tickers=ybatch,
                    period="max",
                    interval="1mo",
                    auto_adjust=False,
                    actions=False,
                    group_by="ticker",
                    threads=True,
                    progress=False,
                )
            except Exception as e:
                msg = str(e)
                if "Rate limited" in msg or "Too Many Requests" in msg or "HTTP Error 429" in msg:
                    wait = pause * (2 ** attempt)
                    throttle(wait)
                    attempt += 1
                    df = None
                    continue
                df = None
                break

        resolved_in_batch = 0
        if isinstance(df, pd.DataFrame) and isinstance(df.columns, pd.MultiIndex):
            # multi-ticker result
            for orig, ysym in zip(batch, ybatch):
                try:
                    c = df[ysym]["Close"].dropna()
                    if len(c) > 0:
                        results[orig] = pd.Timestamp(c.index[0])
                        resolved_in_batch += 1
                except Exception:
                    # will try single-ticker fallback below
                    pass
        elif isinstance(df, pd.DataFrame) and not isinstance(df.columns, pd.MultiIndex) and len(batch) == 1:
            c = df["Close"].dropna()
            if len(c) > 0:
                results[batch[0]] = pd.Timestamp(c.index[0])
                resolved_in_batch += 1

        # Single-ticker fallback for any missing in batch
        missing = [t for t in batch if t not in results]
        for t in missing:
            ysym = yahoo_map[t]
            attempt_i = 0
            while attempt_i <= max_retries and t not in results:
                try:
                    dfi = yf.download(
                        tickers=ysym,
                        period="max",
                        interval="1mo",
                        auto_adjust=False,
                        actions=False,
                        progress=False,
                    )
                    c = dfi["Close"].dropna()
                    if len(c) > 0:
                        results[t] = pd.Timestamp(c.index[0])
                        resolved_in_batch += 1
                        break
                except Exception as e:
                    msg = str(e)
                    if "Rate limited" in msg or "Too Many Requests" in msg or "HTTP Error 429" in msg:
                        wait = pause * (2 ** attempt_i)
                        throttle(wait)
                        attempt_i += 1
                        continue
                # Non-rate-limit or persistent errors: stop retrying this symbol
                break
            throttle(0.1)

        pbar.update(len(batch))
        if verbose:
            pbar.set_postfix({"resolved": len(results)})
        throttle(pause)
    pbar.close()

    out = pd.DataFrame(
        {
            "Symbol": list(results.keys()),
            "EarliestVendorDate": [pd.Timestamp(v).date() for v in results.values()],
        }
    )
    return out


def earliest_vendor_dates_with_cache(
    tickers: Iterable[str],
    cache_path: Optional[Path],
    batch_size: int = 50,
    pause: float = 1.0,
    max_retries: int = 3,
    force_recheck: bool = False,
    verbose: bool = False,
) -> pd.DataFrame:
    """Resolve earliest dates using a CSV cache to avoid re-fetching.

    cache schema: Symbol,EarliestVendorDate
    """
    tickers = [t.strip().upper() for t in tickers if t]
    cache_df = None
    cached = {}
    if cache_path and cache_path.exists() and not force_recheck:
        try:
            cache_df = pd.read_csv(cache_path, dtype={"Symbol": str})
            for _, row in cache_df.iterrows():
                s = str(row["Symbol"]).upper()
                d = row.get("EarliestVendorDate")
                if pd.notna(d):
                    cached[s] = pd.to_datetime(d).date()
        except Exception:
            cache_df = None

    to_fetch = [t for t in tickers if force_recheck or t not in cached]
    if verbose:
        print(f"Cache hits: {len(cached)}; to fetch: {len(to_fetch)}")
    fetched_df = (
        pd.DataFrame(columns=["Symbol", "EarliestVendorDate"]) if not to_fetch else earliest_vendor_dates(
            to_fetch, batch_size=batch_size, pause=pause, max_retries=max_retries, verbose=verbose
        )
    )

    # Merge: prefer fetched for to_fetch, keep cached for others
    merged_map: Dict[str, Optional[pd.Timestamp]] = {}
    for s, d in cached.items():
        merged_map[s] = pd.Timestamp(d)
    if not fetched_df.empty:
        for _, row in fetched_df.iterrows():
            merged_map[str(row["Symbol"]).upper()] = pd.Timestamp(row["EarliestVendorDate"])

    all_rows = sorted([(s, pd.Timestamp(d).date() if d is not None else None) for s, d in merged_map.items()])
    result_df = pd.DataFrame(all_rows, columns=["Symbol", "EarliestVendorDate"]).dropna(subset=["EarliestVendorDate"])

    # Write back cache as union of previous + new
    if cache_path:
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        # Also include any tickers with no date (keep knowledge sparse by excluding NAs)
        result_df.to_csv(cache_path, index=False)

    # Return only rows for requested tickers (so outputs match the current universe)
    return result_df[result_df["Symbol"].isin(tickers)].reset_index(drop=True)
