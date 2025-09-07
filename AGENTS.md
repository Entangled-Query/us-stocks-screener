# Repository Guidelines

## Project Structure & Module Organization
- `us_symbols/` — core Python package
  - `cli.py` (entrypoint), `sources.py` (Nasdaq/SEC), `vendor_yahoo.py` (Yahoo dates), `ipo_nasdaq.py` (IPO calendar), `normalize.py` (ticker mapping), `utils.py` (cache/helpers)
- `data/` — runtime artifacts
  - `inputs/` (user-provided lists), `cache/` (download/vendor/universe caches), `outputs/` (CSV results)
- `requirements.txt` — Python deps; `README.md` — usage; `AGENTS.md` — contributor guide.

## Build, Test, and Development Commands
- Create env + install
  - `python3 -m venv .venv && source .venv/bin/activate && pip install -r requirements.txt`
- Run the CLI (everything + logs)
  - `python -m us_symbols.cli --output-dir data/outputs --batch-size 25 --pause 2.0 --max-retries 4 --verbose`
- Quick smoke test (small list)
  - `python -m us_symbols.cli --symbols-file data/inputs/sample_symbols.csv --output-dir data/outputs --verbose`

## Coding Style & Naming Conventions
- Python, PEP 8, 4‑space indentation, type hints required for public functions.
- Names: modules and functions `snake_case`; classes `PascalCase`; constants `UPPER_SNAKE`.
- Docstrings: concise, include parameter/return types; prefer small, pure helpers.
- Logging: keep stdout quiet by default; use `--verbose` for progress.

## Testing Guidelines
- Framework: `pytest` (add under `tests/` when contributing new logic).
- Tests: name files `test_*.py`; mock network (e.g., `requests_mock`, monkeypatch `yfinance`).
- Focus on normalization, parsing, merging; avoid live calls in CI.

## Commit & Pull Request Guidelines
- Commits: clear, imperative subject (e.g., "add vendor cache seeding").
- PRs: describe motivation, approach, and user‑visible changes (flags, outputs). Include sample commands and snippets of resulting CSV headers.
- Keep diffs small; avoid touching unrelated files. Update `README.md` when behavior or flags change.
- Do not commit `data/cache/` or `data/outputs/` contents unless documenting examples.

## Security & Configuration Tips
- Be gentle with external endpoints (Nasdaq, Yahoo): throttle and handle rate limits. Respect SEC user‑agent rules.
- For proxies/firewalls, rely on system `HTTP(S)_PROXY`. Prefer local files via `--nasdaq-dir` when networks block downloads.

## Agent‑Specific Instructions
- Follow these guidelines for all edits. Preserve structure and style; prefer minimal, targeted changes.
- When adding features: keep flags explicit, document in `README.md`, and ensure caching behavior is unchanged by default.
