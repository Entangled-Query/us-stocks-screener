from __future__ import annotations

import io
from pathlib import Path
from typing import Tuple

import pandas as pd
import requests

from .utils import DEFAULT_CACHE_DIR, read_text_cached, read_json_cached
from .ipo_nasdaq import HEADERS as NASDAQ_HEADERS


NASDQ = "https://www.nasdaqtrader.com/dynamic/SymbolDirectory/nasdaqlisted.txt"
OTHER = "https://www.nasdaqtrader.com/dynamic/SymbolDirectory/otherlisted.txt"
SEC_TICKERS = "https://www.sec.gov/files/company_tickers.json"


def _fetch_text(url: str, timeout: int = 30, headers: dict | None = None) -> str:
    r = requests.get(url, timeout=timeout, headers=headers)
    r.raise_for_status()
    return r.text


def _apply_common_only_filter(df: pd.DataFrame, common_only: bool) -> pd.DataFrame:
    if not common_only:
        return df
    # Heuristic exclusions by name to avoid warrants, units, rights, notes, preferreds, bonds
    excl_terms = [
        "WARRANT", "WARRANTS", "WTS", "WARRANT UNIT",
        "UNIT", "UNITS",
        "RIGHT", "RIGHTS",
        "PREFERRED", "PFD",
        "DEPOSITARY SHARE", "DEPOSITARY SHS",
        "NOTE", "NOTES", "BOND", "BONDS", "DEBENTURE", "DEBENTURES",
        "TRUST PREFERRED",
    ]
    name = df["SecurityName"].astype(str).str.upper()
    mask = ~name.str.contains("|".join([pd.regex.escape(t) for t in excl_terms]), regex=True)
    return df[mask].reset_index(drop=True)


def load_nasdaq_trader(include_etf: bool = False, common_only: bool = True, refresh: bool = False, cache_dir: Path = DEFAULT_CACHE_DIR) -> pd.DataFrame:
    """Load current US symbols from Nasdaq Trader directories.

    Returns columns: Symbol, SecurityName, Exchange
    """
    def load_txt(url: str, cache_name: str) -> pd.DataFrame:
        # First try with headers that prefer plain text
        def fetch_plain():
            return _fetch_text(url, headers={
                "User-Agent": "Mozilla/5.0 (compatible; us-symbols-cli/1.0)",
                "Accept": "text/plain, */*",
            })

        text = read_text_cached(url, cache_file=cache_dir / cache_name, refresh=refresh, fetch_fn=fetch_plain)

        def parse_or_none(txt: str) -> pd.DataFrame | None:
            lines_all = txt.splitlines()
            # Remove known footer line
            lines = [l for l in lines_all if "File Creation Time" not in l]
            # Basic validation: expect at least one '|' in header-ish area
            sample = "\n".join(lines[:5]).strip()
            if "|" not in sample:
                return None
            try:
                return pd.read_csv(io.StringIO("\n".join(lines)), sep="|")
            except Exception:
                return None

        df = parse_or_none(text)
        if df is None:
            # Fallback to FTP host if the main host returned HTML/blocked
            alt = url.replace("www.nasdaqtrader.com", "ftp.nasdaqtrader.com")
            try:
                txt_alt = _fetch_text(alt, headers={
                    "User-Agent": "Mozilla/5.0 (compatible; us-symbols-cli/1.0)",
                    "Accept": "text/plain, */*",
                })
                df = parse_or_none(txt_alt)
                if df is not None:
                    # Overwrite cache with usable content
                    (cache_dir / cache_name).parent.mkdir(parents=True, exist_ok=True)
                    (cache_dir / cache_name).write_text(txt_alt, encoding="utf-8")
            except Exception:
                df = None

        if df is None:
            snippet = (text or "")[:200]
            raise RuntimeError(
                f"Failed to parse Nasdaq Trader list from {url}. First 200 chars: {snippet!r}. "
                "You can retry with --refresh, check network, or provide --symbols-file."
            )
        return df

    ndq = load_txt(NASDQ, "nasdaqlisted.txt")
    oth = load_txt(OTHER, "otherlisted.txt")

    # Normalize column names
    ndq = ndq.rename(columns={"Symbol": "Symbol", "Security Name": "Security Name"})
    oth = oth.rename(columns={"ACT Symbol": "Symbol", "Security Name": "Security Name"})

    # Filter ETFs and tests
    if not include_etf:
        ndq = ndq[(ndq.get("ETF", "N") == "N") & (ndq.get("Test Issue", "N") == "N")]
        oth = oth[(oth.get("ETF", "N") == "N") & (oth.get("Test Issue", "N") == "N")]
    else:
        ndq = ndq[ndq.get("Test Issue", "N") == "N"]
        oth = oth[oth.get("Test Issue", "N") == "N"]

    ndq_out = pd.DataFrame(
        {
            "Symbol": ndq["Symbol"].astype(str).str.upper(),
            "SecurityName": ndq["Security Name"].astype(str),
            "Exchange": "NASDAQ",
        }
    )
    # Map otherlisted Exchange code to human-readable
    exch_map = {
        "A": "AMEX",
        "N": "NYSE",
        "P": "ARCA",
        "Z": "BATS",
        "V": "IEX",
    }
    oth_out = pd.DataFrame(
        {
            "Symbol": oth["Symbol"].astype(str).str.upper(),
            "SecurityName": oth["Security Name"].astype(str),
            "Exchange": oth.get("Exchange", "").astype(str).map(exch_map).fillna("NYSE/AMEX"),
        }
    )

    combined = pd.concat([ndq_out, oth_out], ignore_index=True)
    combined = combined.drop_duplicates("Symbol").reset_index(drop=True)
    combined = _apply_common_only_filter(combined, common_only=common_only)
    return combined


def load_nasdaq_trader_from_dir(path: Path, include_etf: bool = False, common_only: bool = True) -> pd.DataFrame:
    """Parse local nasdaqtrader files nasdaqlisted.txt and otherlisted.txt."""
    p1 = path / "nasdaqlisted.txt"
    p2 = path / "otherlisted.txt"
    if not p1.exists() or not p2.exists():
        raise FileNotFoundError("Expected nasdaqlisted.txt and otherlisted.txt in provided directory")

    def read_txt(p: Path) -> pd.DataFrame:
        lines = [l for l in p.read_text(encoding="utf-8", errors="ignore").splitlines() if "File Creation Time" not in l]
        return pd.read_csv(io.StringIO("\n".join(lines)), sep="|")

    ndq = read_txt(p1).rename(columns={"Symbol": "Symbol", "Security Name": "Security Name"})
    oth = read_txt(p2).rename(columns={"ACT Symbol": "Symbol", "Security Name": "Security Name"})

    if not include_etf:
        ndq = ndq[(ndq.get("ETF", "N") == "N") & (ndq.get("Test Issue", "N") == "N")]
        oth = oth[(oth.get("ETF", "N") == "N") & (oth.get("Test Issue", "N") == "N")]
    else:
        ndq = ndq[ndq.get("Test Issue", "N") == "N"]
        oth = oth[oth.get("Test Issue", "N") == "N"]

    exch_map = {
        "A": "AMEX",
        "N": "NYSE",
        "P": "ARCA",
        "Z": "BATS",
        "V": "IEX",
    }

    ndq_out = pd.DataFrame({
        "Symbol": ndq["Symbol"].astype(str).str.upper(),
        "SecurityName": ndq["Security Name"].astype(str),
        "Exchange": "NASDAQ",
    })
    oth_out = pd.DataFrame({
        "Symbol": oth["Symbol"].astype(str).str.upper(),
        "SecurityName": oth["Security Name"].astype(str),
        "Exchange": oth.get("Exchange", "").astype(str).map(exch_map).fillna("NYSE/AMEX"),
    })

    combined = pd.concat([ndq_out, oth_out], ignore_index=True)
    combined = combined.drop_duplicates("Symbol").reset_index(drop=True)
    combined = _apply_common_only_filter(combined, common_only=common_only)
    return combined


def load_nasdaq_screener(include_etf: bool = False, common_only: bool = True, refresh: bool = False, cache_dir: Path = DEFAULT_CACHE_DIR) -> pd.DataFrame:
    """Fallback: use Nasdaq public screener JSON per exchange.

    This endpoint is undocumented and may change. We heuristically exclude ETFs by name.
    Returns columns: Symbol, SecurityName, Exchange
    """
    exchanges = [
        ("nasdaq", "NASDAQ"),
        ("nyse", "NYSE"),
        ("amex", "AMEX"),
    ]
    frames = []
    for ex_param, ex_name in exchanges:
        # Use a large limit to avoid truncation
        url = f"https://api.nasdaq.com/api/screener/stocks?tableonly=true&limit=20000&exchange={ex_param}"

        def fetch():
            r = requests.get(url, headers=NASDAQ_HEADERS, timeout=30)
            r.raise_for_status()
            return r.json()

        j = read_json_cached(url, cache_dir / f"screener_{ex_param}.json", refresh=refresh, fetch_fn=fetch)
        rows = []
        try:
            # Try two known shapes
            data = j.get("data", {})
            rows = data.get("rows") or data.get("table", {}).get("rows") or []
            # If metadata suggests more rows than returned, warn via print
            try:
                total = int(data.get("totalRecords") or data.get("recordsTotal") or 0)
                if total and rows and len(rows) < total:
                    print(f"Warning: Screener {ex_param} returned {len(rows)} < total {total}; results may be truncated.")
            except Exception:
                pass
        except Exception:
            rows = []
        if not rows:
            continue
        recs = []
        for row in rows:
            sym = (row.get("symbol") or row.get("Symbol") or "").strip().upper()
            name = (row.get("name") or row.get("companyName") or row.get("securityName") or "").strip()
            if not sym:
                continue
            if not include_etf:
                nm = name.upper()
                if " ETF" in nm or nm.endswith("ETF") or " ETN" in nm or nm.endswith("ETN"):
                    continue
            if common_only:
                nm = name.upper()
                if any(x in nm for x in [
                    "WARRANT", "WARRANTS", "WTS", "WARRANT UNIT", "UNIT", "UNITS",
                    "RIGHT", "RIGHTS", "PREFERRED", "PFD", "DEPOSITARY SHARE", "DEPOSITARY SHS",
                    "NOTE", "NOTES", "BOND", "BONDS", "DEBENTURE", "DEBENTURES", "TRUST PREFERRED",
                ]):
                    continue
            recs.append({"Symbol": sym, "SecurityName": name, "Exchange": ex_name})
        if recs:
            frames.append(pd.DataFrame(recs))

    if not frames:
        raise RuntimeError("Nasdaq screener fallback returned no rows. Network may be blocked.")
    df = pd.concat(frames, ignore_index=True)
    df = df.drop_duplicates("Symbol").reset_index(drop=True)
    return df


def load_sec_cik_map(refresh: bool = False, cache_dir: Path = DEFAULT_CACHE_DIR) -> pd.DataFrame:
    """Optional SEC CIK mapping: returns columns Symbol, CIK."""
    def fetch():
        r = requests.get(
            SEC_TICKERS,
            headers={
                "User-Agent": "Mozilla/5.0 (compatible; us-symbols-cli/1.0; +https://example.com)",
                "Accept": "application/json, text/plain, */*",
                "Cache-Control": "no-cache",
            },
            timeout=30,
        )
        r.raise_for_status()
        return r.text

    text = read_text_cached(SEC_TICKERS, cache_dir / "sec_company_tickers.json", refresh=refresh, fetch_fn=fetch)
    data = pd.read_json(io.StringIO(text), orient="index")
    # data columns: cik_str, ticker, title
    out = data[["ticker", "cik_str"]].rename(columns={"ticker": "Symbol", "cik_str": "CIK"})
    out["Symbol"] = out["Symbol"].str.upper()
    return out
