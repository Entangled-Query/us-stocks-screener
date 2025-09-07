from __future__ import annotations


def to_yahoo_symbol(symbol: str) -> str:
    """Map exchange-style symbols to Yahoo style where needed.

    Rules implemented:
    - Share classes: replace '.' or '/' with '-' (e.g., BRK.B, BRK/B -> BRK-B)
    - Preferred shares: caret to -P suffix (e.g., NLY^F -> NLY-PF)
    - Units/Rights/Warrants suffix with slash -> map to '-U', '-RT', '-WS' (best-effort)
    """
    s = symbol.strip().upper()
    # Preferred shares: TICKER^A -> TICKER-PA
    if '^' in s:
        base, suf = s.split('^', 1)
        suf = suf.strip()
        if suf:
            return f"{base}-P{suf}"
        return base
    # Warrants/Rights/Units with slash suffix
    for suf, rep in (('/WS', '-WS'), ('/W', '-W'), ('/WT', '-WT'), ('/RT', '-RT'), ('/U', '-U')):
        if s.endswith(suf):
            return s.replace('/', '-').replace('.', '-')
    # Generic class shares: dots and slashes to hyphen
    s = s.replace('.', '-').replace('/', '-')
    return s


def from_yahoo_symbol(symbol: str) -> str:
    """Reverse mapping (best-effort)."""
    s = symbol.replace('-', '.')
    return s
