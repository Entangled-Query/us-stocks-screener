from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from .sources import load_nasdaq_trader, load_sec_cik_map
from .vendor_yahoo import earliest_vendor_dates
from .ipo_nasdaq import fetch_nasdaq_ipo_range
from .utils import DEFAULT_CACHE_DIR, DEFAULT_OUTPUT_DIR, ensure_dir


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="US symbols + earliest market date (free)")
    # Defaults: fetch everything (ETFs + non-common included)
    p.add_argument("--exclude-etf", action="store_true", help="Exclude ETFs/ETNs (default: include)")
    p.add_argument("--common-only", action="store_true", help="Only common stocks (exclude warrants/rights/units/notes/preferred)")
    p.add_argument("--with-ipo", action="store_true", help="Include Nasdaq IPO pricing dates")
    p.add_argument("--ipo-start-year", type=int, default=1998, help="Nasdaq IPO start year (default: 1998)")
    p.add_argument("--ipo-end-year", type=int, help="Nasdaq IPO end year (default: current)")
    p.add_argument("--ipo-end-month", type=int, choices=range(1,13), help="Nasdaq IPO end month (1-12; default: current month)")
    p.add_argument("--batch-size", type=int, default=50, help="Yahoo batch size (default: 50)")
    p.add_argument("--pause", type=float, default=1.5, help="Seconds to pause between Yahoo batches (default: 1.5)")
    p.add_argument("--max-retries", type=int, default=3, help="Max retries on Yahoo rate limits (default: 3)")
    p.add_argument("--refresh", action="store_true", help="Refresh cached downloads")
    p.add_argument("--output-dir", type=str, default=str(DEFAULT_OUTPUT_DIR), help="Output directory for CSVs")
    p.add_argument("--no-sec", action="store_true", help="Skip SEC CIK enrichment")
    p.add_argument("--symbols-file", type=str, help="Optional CSV with a Symbol column to use instead of fetching from Nasdaq Trader")
    p.add_argument("--nasdaq-dir", type=str, help="Directory containing nasdaqlisted.txt and otherlisted.txt to parse locally")
    p.add_argument("--vendor-cache", type=str, default="data/cache/vendor/earliest_yahoo.csv", help="CSV cache for earliest Yahoo dates")
    p.add_argument("--force-recheck", action="store_true", help="Ignore vendor cache and re-fetch all symbols")
    p.add_argument("--verbose", action="store_true", help="Print progress logs while running")
    p.add_argument("--universe-cache", type=str, default="data/cache/universe_cache.csv", help="Cache of merged universe with names, exchange, CIK, earliest date, IPO date")
    # Cache validation options
    p.add_argument("--validate-cache-sample", type=float, default=0.05, help="Fraction of cached symbols to re-check against Yahoo (default: 0.05)")
    p.add_argument("--validate-cache-min", type=int, default=20, help="Minimum number of symbols to validate from cache (default: 20)")
    p.add_argument("--validate-cache-output", type=str, default="data/outputs/vendor_cache_mismatches.csv", help="CSV path to write cache validation mismatches")
    p.add_argument("--skip-validate", action="store_true", help="Skip validating a random sample of cached symbols")
    return p


def main() -> None:
    args = build_arg_parser().parse_args()
    out_dir = Path(args.output_dir)
    ensure_dir(out_dir)

    # 1) Load current US symbols or from file
    if args.verbose:
        print("[1/4] Loading US symbols...")
    if args.symbols_file:
        pth = Path(args.symbols_file)
        df = pd.read_csv(pth)
        if "Symbol" not in df.columns:
            raise SystemExit("symbols-file must have a 'Symbol' column")
        # normalize minimal schema
        symbols_df = pd.DataFrame({
            "Symbol": df["Symbol"].astype(str).str.upper(),
            "SecurityName": df.get("SecurityName", pd.Series([None]*len(df))),
            "Exchange": df.get("Exchange", pd.Series([None]*len(df))),
        })
    else:
        if args.nasdaq_dir:
            from .sources import load_nasdaq_trader_from_dir
            symbols_df = load_nasdaq_trader_from_dir(Path(args.nasdaq_dir), include_etf=not args.exclude_etf, common_only=not args.common_only)
        else:
            # Try Nasdaq Trader; on failure, fallback to Nasdaq Screener
            try:
                symbols_df = load_nasdaq_trader(include_etf=not args.exclude_etf, common_only=not args.common_only, refresh=args.refresh)
            except Exception as e:
                print(f"Nasdaq Trader fetch failed: {e}\nFalling back to Nasdaq Screener API...")
                from .sources import load_nasdaq_screener
                try:
                    symbols_df = load_nasdaq_screener(include_etf=not args.exclude_etf, common_only=not args.common_only, refresh=args.refresh)
                except Exception as e2:
                    raise SystemExit(
                        f"Nasdaq screener fallback also failed: {e2}.\n"
                        "Please try again with --nasdaq-dir pointing to local nasdaqlisted.txt and otherlisted.txt, "
                        "or use --symbols-file to provide a custom list."
                    )
    symbols_df["ListedCurrently"] = True
    symbols_df.to_csv(out_dir / "us_symbols.csv", index=False)

    # 2) SEC CIK enrichment (optional)
    if not args.no_sec:
        try:
            sec_df = load_sec_cik_map(refresh=args.refresh)
            symbols_df = symbols_df.merge(sec_df, on="Symbol", how="left")
        except Exception:
            # Continue without SEC mapping if it fails
            symbols_df["CIK"] = pd.NA

    print(f"Symbols loaded: {len(symbols_df)} unique tickers after filters.")

    # Seed vendor cache from universe cache (if present), so we reuse earliest dates
    ucache_path = Path(args.universe_cache) if args.universe_cache else None
    vcache_path = Path(args.vendor_cache) if args.vendor_cache else None
    if ucache_path and ucache_path.exists() and vcache_path and not args.force_recheck:
        try:
            udf = pd.read_csv(ucache_path)
            if {"Symbol", "EarliestVendorDate"}.issubset(udf.columns):
                # Build a union with existing vendor cache (if any)
                u = udf[["Symbol", "EarliestVendorDate"]].dropna(subset=["Symbol", "EarliestVendorDate"]).copy()
                u["Symbol"] = u["Symbol"].astype(str).str.upper()
                u["EarliestVendorDate"] = pd.to_datetime(u["EarliestVendorDate"], errors="coerce").dt.date
                u = u.dropna(subset=["EarliestVendorDate"]).groupby("Symbol", as_index=False)["EarliestVendorDate"].min()

                if vcache_path.exists():
                    v = pd.read_csv(vcache_path)
                    if {"Symbol", "EarliestVendorDate"}.issubset(v.columns):
                        v["Symbol"] = v["Symbol"].astype(str).str.upper()
                        v["EarliestVendorDate"] = pd.to_datetime(v["EarliestVendorDate"], errors="coerce").dt.date
                        v = v.dropna(subset=["EarliestVendorDate"]).groupby("Symbol", as_index=False)["EarliestVendorDate"].min()
                        u = pd.concat([u, v], ignore_index=True).dropna().groupby("Symbol", as_index=False)["EarliestVendorDate"].min()
                vcache_path.parent.mkdir(parents=True, exist_ok=True)
                u.to_csv(vcache_path, index=False)
                if args.verbose:
                    print(f"Seeded vendor cache from universe cache: {len(u)} symbols")
        except Exception:
            pass
    # Optional: validate a random sample of cached symbols against fresh Yahoo queries
    if (
        not args.skip_validate
        and vcache_path
        and vcache_path.exists()
        and not args.force_recheck
        and args.validate_cache_sample > 0
    ):
        try:
            import random, math
            from .vendor_yahoo import earliest_vendor_dates as _fresh_fetch

            vdf = pd.read_csv(vcache_path)
            if {"Symbol", "EarliestVendorDate"}.issubset(vdf.columns):
                vdf["Symbol"] = vdf["Symbol"].astype(str).str.upper()
                vdf["EarliestVendorDate"] = pd.to_datetime(vdf["EarliestVendorDate"], errors="coerce").dt.date
                cached_map = dict(vdf.dropna(subset=["EarliestVendorDate"]).set_index("Symbol")["EarliestVendorDate"])
                universe_syms = set(symbols_df["Symbol"].astype(str).str.upper())
                candidates = [s for s in cached_map.keys() if s in universe_syms]
                if candidates:
                    k = max(int(math.ceil(len(candidates) * args.validate_cache_sample)), args.validate_cache_min)
                    k = min(k, len(candidates))
                    sample = random.sample(candidates, k)
                    if args.verbose:
                        print(f"[validate] Sampling {k} of {len(candidates)} cached symbols for verification...")
                    fresh = _fresh_fetch(sample, batch_size=min(args.batch_size, 50), pause=args.pause, max_retries=args.max_retries, verbose=args.verbose)
                    if not fresh.empty:
                        fresh["Symbol"] = fresh["Symbol"].astype(str).str.upper()
                        fresh["EarliestVendorDate"] = pd.to_datetime(fresh["EarliestVendorDate"], errors="coerce").dt.date
                        merged_chk = fresh.merge(vdf[["Symbol", "EarliestVendorDate"]].rename(columns={"EarliestVendorDate": "CachedDate"}), on="Symbol", how="left")
                        merged_chk = merged_chk.rename(columns={"EarliestVendorDate": "FreshDate"})
                        mm = merged_chk[(merged_chk["FreshDate"].notna()) & (merged_chk["CachedDate"].notna()) & (merged_chk["FreshDate"] != merged_chk["CachedDate"])]
                        outp = Path(args.validate_cache_output)
                        outp.parent.mkdir(parents=True, exist_ok=True)
                        if not mm.empty:
                            mm.to_csv(outp, index=False)
                            print(f"[validate] WARNING: {len(mm)} mismatches found. See {outp}")
                        else:
                            # Write an empty file with headers to indicate run completed validation
                            mm.to_csv(outp, index=False)
                            if args.verbose:
                                print(f"[validate] Cache OK for {len(fresh)} checked symbols.")
        except Exception as _e:
            if args.verbose:
                print(f"[validate] Skipped due to error: {_e}")

    # 3) Yahoo earliest vendor dates
    from .vendor_yahoo import earliest_vendor_dates_with_cache
    if args.verbose:
        print("[2/4] Resolving earliest vendor dates from Yahoo...")
    earliest_df = earliest_vendor_dates_with_cache(
        symbols_df["Symbol"].tolist(),
        cache_path=Path(args.vendor_cache) if args.vendor_cache else None,
        batch_size=args.batch_size,
        pause=args.pause,
        max_retries=args.max_retries,
        force_recheck=args.force_recheck,
        verbose=args.verbose,
    )
    earliest_df.to_csv(out_dir / "earliest_vendor_dates.csv", index=False)
    # Save list of symbols with no vendor date (e.g., warrants/notes/intraday-only on Yahoo)
    missing = symbols_df.loc[~symbols_df["Symbol"].isin(earliest_df["Symbol"])].copy()
    if not missing.empty:
        missing.to_csv(out_dir / "earliest_vendor_dates_missing.csv", index=False)

    merged = symbols_df.merge(earliest_df, on="Symbol", how="left")

    # 4) Optional: Nasdaq IPO pricing calendar
    if args.with_ipo:
        if args.verbose:
            print("[3/4] Fetching Nasdaq IPO calendar...")
        try:
            ipo_df = fetch_nasdaq_ipo_range(
                start_year=args.ipo_start_year,
                end_year=args.ipo_end_year,
                end_month=args.ipo_end_month,
                refresh=args.refresh,
            )
            ipo_df.to_csv(out_dir / "ipo_calendar.csv", index=False)

            # Join IPO dates and also add IPO-only tickers (may be delisted or not in current lists)
            merged = merged.merge(ipo_df[["Symbol", "IPODate"]].drop_duplicates("Symbol"), on="Symbol", how="left")

            # IPO-only symbols not in current lists
            missing = ipo_df[~ipo_df["Symbol"].isin(merged["Symbol"])][["Symbol", "Company", "IPODate"]].drop_duplicates("Symbol")
            if not missing.empty:
                missing = missing.rename(columns={"Company": "SecurityName"})
                missing["Exchange"] = pd.NA
                missing["ListedCurrently"] = False
                missing["CIK"] = pd.NA
                missing["EarliestVendorDate"] = pd.NA
                merged = pd.concat([merged, missing[merged.columns]], ignore_index=True)
        except Exception:
            # IPO enrichment failed; proceed with vendor dates only
            pass

    # 5) Write final merged file
    if args.verbose:
        print("[4/4] Writing merged outputs...")
    merged.to_csv(out_dir / "us_symbols_merged.csv", index=False)
    print(f"Done. Outputs written under: {out_dir}")

    # Update universe cache with richer information
    try:
        if ucache_path:
            ucache_path.parent.mkdir(parents=True, exist_ok=True)
            if ucache_path.exists():
                prev = pd.read_csv(ucache_path)
            else:
                prev = pd.DataFrame(columns=merged.columns)

            # Combine previous and current, preferring non-null names/exchange/CIK from current, and minimum dates
            cols = list({*prev.columns, *merged.columns})
            prev = prev.reindex(columns=cols)
            cur = merged.reindex(columns=cols)
            combo = pd.concat([prev, cur], ignore_index=True)

            # Normalize types
            if "Symbol" in combo.columns:
                combo["Symbol"] = combo["Symbol"].astype(str).str.upper()
            for dtcol in ["EarliestVendorDate", "IPODate"]:
                if dtcol in combo.columns:
                    combo[dtcol] = pd.to_datetime(combo[dtcol], errors="coerce").dt.date

            # Aggregate rules per symbol
            def first_nonnull(series):
                for v in series:
                    if pd.notna(v):
                        return v
                return pd.NA

            agg = {
                "SecurityName": first_nonnull,
                "Exchange": first_nonnull,
                "CIK": first_nonnull,
                "ListedCurrently": first_nonnull,
                "EarliestVendorDate": "min",
                "IPODate": "min",
            }
            # Only include keys present
            agg_used = {k: v for k, v in agg.items() if k in combo.columns}
            unified = combo.groupby("Symbol", as_index=False).agg(agg_used)
            unified.to_csv(ucache_path, index=False)
            if args.verbose:
                print(f"Universe cache updated: {len(unified)} symbols -> {ucache_path}")
    except Exception:
        pass


if __name__ == "__main__":
    main()
