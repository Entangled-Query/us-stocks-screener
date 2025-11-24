US Symbols + Earliest Market Date Screener

[![Weekly Symbols Refresh](https://github.com/Entangled-Query/us-stocks-screener/actions/workflows/weekly-update.yml/badge.svg)](https://github.com/Entangled-Query/us-stocks-screener/actions/workflows/weekly-update.yml)

Overview
- Produces a unified list of current US stock symbols and a practical earliest market date for backfilling prices using free sources.
- Optionally augments with IPO pricing dates from Nasdaq’s public calendar.
- No subscriptions. Uses: Nasdaq Trader, Yahoo (via yfinance), and optional Nasdaq IPO JSON.

Outputs
- data/outputs/us_symbols.csv — Current symbols and names (from Nasdaq Trader or Screener fallback).
- data/outputs/earliest_vendor_dates.csv — Earliest Yahoo vendor dates per ticker.
- data/outputs/ipo_calendar.csv — Optional Nasdaq IPO pricing calendar (historical priced IPOs).
- data/outputs/us_symbols_merged.csv — Final merged dataset with Symbol, SecurityName, Exchange, CIK (if available), EarliestVendorDate, IPODate, ListedCurrently.

Which File To Use
- Use `us_symbols_merged.csv` as the final, ready-to-consume table.
- `us_symbols.csv` is the universe list; `earliest_vendor_dates.csv` is the (Symbol → earliest date) map; `earliest_vendor_dates_missing.csv` lists unresolved tickers.

Quick Start
1) Create venv and install deps
   python -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt

2) Run the pipeline (everything + progress logs)
   python -m us_symbols.cli --output-dir data/outputs --batch-size 25 --pause 2.0 --max-retries 4 --verbose

3) Include IPO pricing dates (Nasdaq calendar) from 1998 onward
   python -m us_symbols.cli --with-ipo --ipo-start-year 1998 --output-dir data/outputs --verbose

If network to Nasdaq/SEC is restricted, you can provide your own list:
   # CSV with at least a Symbol column
   python -m us_symbols.cli --symbols-file data/inputs/sample_symbols.csv --output-dir data/outputs

Or use locally downloaded Nasdaq Trader files (no network during run):
   # Place nasdaqlisted.txt and otherlisted.txt in data/inputs/nasdaqtrader/
   python -m us_symbols.cli --nasdaq-dir data/inputs/nasdaqtrader --output-dir data/outputs

To create that directory yourself:
   curl -L -o data/inputs/nasdaqtrader/nasdaqlisted.txt https://www.nasdaqtrader.com/dynamic/SymbolDirectory/nasdaqlisted.txt
   curl -L -o data/inputs/nasdaqtrader/otherlisted.txt https://www.nasdaqtrader.com/dynamic/SymbolDirectory/otherlisted.txt

Useful Flags
- Defaults fetch everything (ETFs + non-common). Use these to narrow:
  - --exclude-etf: exclude ETFs/ETNs
  - --common-only: only common stocks (exclude warrants/rights/units/notes/preferred)
- --batch-size: Yahoo batch size (default: 50)
- --pause: seconds between Yahoo batches (default: 1.5)
- --max-retries: retries on Yahoo rate limits (default: 3)
- --refresh: refresh cached downloads (default: off)
- --output-dir: where to write CSVs (default: data/outputs)
- --no-sec: skip SEC CIK enrichment
- --symbols-file: provide your own list instead of fetching
- --nasdaq-dir: parse local nasdaqlisted.txt + otherlisted.txt
- --vendor-cache: CSV path to cache earliest Yahoo dates (default: data/cache/vendor/earliest_yahoo.csv)
- --force-recheck: ignore vendor cache and re-fetch all symbols
 - --universe-cache: cache of merged universe with metadata (default: data/cache/universe_cache.csv)
 - --verbose: print progress logs and Yahoo progress bar

Notes & Limitations
- Current listings only: Nasdaq Trader lists do not include delisted symbols. IPO calendar can add some delisted names, marked as ListedCurrently=False.
- EarliestVendorDate is the earliest date Yahoo has data for a ticker; it is a practical backfill start, not guaranteed to equal IPO date.
- Nasdaq IPO API is undocumented and may change; headers are added and calls are throttled.
- If Nasdaq Trader returns an HTML landing page or is blocked, the CLI will automatically fall back to the Nasdaq Screener API. If both are blocked, use --nasdaq-dir or --symbols-file.
- Ticker normalization: Yahoo uses dashes for share classes (e.g., BRK-B). This tool maps dots to dashes when querying Yahoo.

Design Highlights (Best Practice, Not Overbuilt)
- Clear separation: sources (Nasdaq/SEC), vendor (Yahoo), IPO (Nasdaq), normalization, and CLI orchestrator.
- Resumable caching: raw downloads are cached under data/cache to avoid re-downloading unless --refresh is set.
- Safe defaults: excludes ETFs and test issues; throttles queries; handles partial failures and continues.
- Extensible: additional vendors or sources can be added with minimal changes.

Caching
- Vendor cache: `data/cache/vendor/earliest_yahoo.csv` stores Symbol → EarliestVendorDate and skips re-fetching on re-runs.
- Universe cache: `data/cache/universe_cache.csv` stores merged metadata (name, exchange, CIK, earliest date, IPO date) across runs.
- Use `--force-recheck` to ignore caches and query everything again.

Progress & Logs
- Add `--verbose` to see step logs and a tqdm progress bar for Yahoo.
- Shows cache hits vs. to-fetch, resolved counts, and final summary.

Examples
- Everything (default):
  - `python -m us_symbols.cli --output-dir data/outputs --batch-size 25 --pause 2.0 --max-retries 4 --verbose`
- Common stocks only:
  - `python -m us_symbols.cli --output-dir data/outputs --common-only --verbose`
- Exclude ETFs but include other non-common:
  - `python -m us_symbols.cli --output-dir data/outputs --exclude-etf --verbose`

Symbol Normalization (Yahoo)
- Share classes: `.` and `/` → `-` (e.g., `BRK/B` → `BRK-B`).
- Preferred series: `^A` → `-PA` (e.g., `NLY^F` → `NLY-PF`).
- Warrants/Rights/Units: common suffixes mapped to hyphen forms (best-effort; many lack long history).

Troubleshooting
- Trader HTML/WAF: CLI auto-falls back to Screener; otherwise use `--nasdaq-dir` with local files.
- Yahoo rate limits: lower `--batch-size`, increase `--pause`, raise `--max-retries`; cache avoids repeating work.
- Missing vendor dates: see `earliest_vendor_dates_missing.csv`; often non-common instruments or transient vendor issues.
