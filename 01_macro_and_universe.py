import time

import pandas as pd
import requests
import yfinance as yf
from bs4 import BeautifulSoup  # noqa: F401 — available for future HTML parsing

_UA_HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

_MACRO_TICKERS = {
    "Crude_Oil":        "CL=F",
    "Gold":             "GC=F",
    "10Y_Treasury":     "^TNX",
    "VIX":              "^VIX",
}


def _read_wiki(url: str) -> list[pd.DataFrame]:
    """Fetches all HTML tables from a Wikipedia URL with a browser User-Agent."""
    return pd.read_html(url, storage_options=_UA_HEADERS)


_INTL_SUFFIXES = {
    "DE", "PA", "L", "MI", "AS", "MC", "BR", "VI", "HE", "ST",
    "CO", "OL", "LS", "SW", "VX", "IR", "AT", "WA", "PR",
}

_US_DUAL_CLASS = {
    "BRK-A", "BRK-B", "BF-A", "BF-B",
}


def _format_ticker(raw: str) -> str:
    """
    Converts a raw Wikipedia ticker to the correct Yahoo Finance format:

    - US dual-class shares (BRK-B, BF-B) keep their hyphen.
    - International exchange suffixes (DE, PA, L, MI, AS …) use a dot
      separator: EOAN.DE, MC.PA, ULVR.L.
    - Plain US tickers with a dot (BRK.B from some tables) are converted
      to hyphen ONLY when they are known dual-class shares.
    - All other dots are left as-is (Yahoo Finance accepts them).
    """
    t = str(raw).strip()
    if not t:
        return ""

    upper = t.upper()

    if upper in _US_DUAL_CLASS:
        return upper

    if "." in t:
        parts = t.rsplit(".", 1)
        suffix = parts[1].upper()
        if suffix in _INTL_SUFFIXES:
            return f"{parts[0].upper()}.{suffix}"
        candidate = f"{parts[0].upper()}-{suffix}"
        if candidate in _US_DUAL_CLASS:
            return candidate
        return t

    if "-" in t:
        parts = t.rsplit("-", 1)
        suffix = parts[1].upper()
        if suffix in _INTL_SUFFIXES:
            return f"{parts[0].upper()}.{suffix}"
        return t.upper()

    return upper


def _clean_tickers(tickers: list[str]) -> list[str]:
    """Formats a list of raw tickers and drops empty results."""
    seen: set[str] = set()
    result = []
    for raw in tickers:
        t = _format_ticker(raw)
        if t and t not in seen:
            seen.add(t)
            result.append(t)
    return result


def get_global_universe() -> pd.DataFrame:
    """
    Scrapes S&P 500, Nasdaq 100, DJIA, DAX, and Euro Stoxx 50 constituents
    from Wikipedia, deduplicates, and saves to global_universe.csv.
    Returns a DataFrame with columns: ticker, index.
    """
    records = []

    sp500_tables = _read_wiki("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies")
    for ticker in _clean_tickers(sp500_tables[0]["Symbol"].tolist()):
        records.append({"ticker": ticker, "index": "S&P500"})
    time.sleep(0.5)

    ndx_tables = _read_wiki("https://en.wikipedia.org/wiki/Nasdaq-100")
    ndx_df = next((t for t in ndx_tables if "Ticker" in t.columns), None)
    if ndx_df is not None:
        for ticker in _clean_tickers(ndx_df["Ticker"].tolist()):
            records.append({"ticker": ticker, "index": "Nasdaq100"})
    time.sleep(0.5)

    djia_tables = _read_wiki("https://en.wikipedia.org/wiki/Dow_Jones_Industrial_Average")
    djia_df = next((t for t in djia_tables if "Symbol" in t.columns), None)
    if djia_df is not None:
        for ticker in _clean_tickers(djia_df["Symbol"].tolist()):
            records.append({"ticker": ticker, "index": "DJIA"})
    time.sleep(0.5)

    dax_tables = _read_wiki("https://en.wikipedia.org/wiki/DAX")
    dax_df = next(
        (t for t in dax_tables if any(c in t.columns for c in ["Ticker", "Symbol", "Tickersymbol"])),
        None,
    )
    if dax_df is not None:
        col = next(c for c in ["Ticker", "Symbol", "Tickersymbol"] if c in dax_df.columns)
        for ticker in _clean_tickers(dax_df[col].tolist()):
            if "." not in ticker and "-" not in ticker:
                ticker = f"{ticker}.DE"
            records.append({"ticker": ticker, "index": "DAX"})
    time.sleep(0.5)

    stoxx_tables = _read_wiki("https://en.wikipedia.org/wiki/Euro_Stoxx_50")
    stoxx_df = next(
        (t for t in stoxx_tables if any(c in t.columns for c in ["Ticker", "Symbol"])),
        None,
    )
    if stoxx_df is not None:
        col = next(c for c in ["Ticker", "Symbol"] if c in stoxx_df.columns)
        for ticker in _clean_tickers(stoxx_df[col].tolist()):
            records.append({"ticker": ticker, "index": "EuroStoxx50"})

    df = pd.DataFrame(records)
    df.drop_duplicates(subset="ticker", keep="first", inplace=True)
    df.sort_values("ticker", inplace=True)
    df.reset_index(drop=True, inplace=True)

    df.to_csv("global_universe.csv", index=False)
    return df


def benner_cycle_phase(year: int = None) -> dict:
    """
    Computes the current Benner Cycle phase.

    Samuel Benner (1875) identified a repeating ~27-year cycle in commodity
    prices with three phases:
        - PANIC / BUY  : years of lowest prices — best time to buy
        - GOOD TIMES   : rising prices — hold / accumulate
        - BOOM / SELL  : years of highest prices — best time to sell

    The pattern repeats in a 16-18-20 year sequence for highs and
    8-9-10 year sequence for lows, anchored to known historical turning points.

    Known panic lows (buy years):  1902, 1911, 1920, 1929, 1949, 1965, 1982, 1999, 2018
    Known boom highs (sell years): 1906, 1916, 1929, 1937, 1955, 1973, 1987, 2010, 2026
    """
    import datetime
    if year is None:
        year = datetime.datetime.now().year

    panic_lows  = [1902, 1911, 1920, 1929, 1949, 1965, 1982, 1999, 2018, 2036]
    boom_highs  = [1906, 1916, 1929, 1937, 1955, 1973, 1987, 2010, 2026, 2044]

    dist_panic = min(panic_lows, key=lambda y: abs(y - year))
    dist_boom  = min(boom_highs, key=lambda y: abs(y - year))

    years_to_panic = dist_panic - year
    years_to_boom  = dist_boom  - year

    if abs(years_to_panic) <= 1:
        phase  = "PANIC — ZONE D'ACHAT MAXIMALE"
        signal = "BUY"
        color  = "green"
    elif abs(years_to_boom) <= 1:
        phase  = "BOOM — ZONE DE VENTE MAXIMALE"
        signal = "SELL"
        color  = "red"
    elif years_to_panic > 0 and (years_to_boom < 0 or years_to_panic < years_to_boom):
        phase  = "GOOD TIMES — Phase de montée"
        signal = "HOLD/ACCUMULATE"
        color  = "blue"
    elif years_to_boom > 0:
        phase  = "GOOD TIMES — Approche du sommet"
        signal = "REDUCE/CAUTION"
        color  = "orange"
    else:
        phase  = "TRANSITION"
        signal = "NEUTRAL"
        color  = "grey"

    next_panic = next((y for y in sorted(panic_lows) if y >= year), None)
    next_boom  = next((y for y in sorted(boom_highs)  if y >= year), None)

    return {
        "Benner_Year":        year,
        "Benner_Phase":       phase,
        "Benner_Signal":      signal,
        "Benner_Color":       color,
        "Benner_Next_Panic":  next_panic,
        "Benner_Next_Boom":   next_boom,
        "Benner_Yrs_To_Panic": years_to_panic if next_panic else None,
        "Benner_Yrs_To_Boom":  years_to_boom  if next_boom  else None,
    }


def _fetch_fear_greed() -> dict:
    """
    Fetches the CNN Fear & Greed Index (current score + rating).
    Returns dict with keys: Fear_Greed_Score (0-100), Fear_Greed_Rating (str).
    Falls back to None on any error.
    """
    try:
        url = "https://production.dataviz.cnn.io/index/fearandgreed/graphdata"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
            "Referer": "https://www.cnn.com/markets/fear-and-greed",
        }
        resp = requests.get(url, headers=headers, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        score  = data["fear_and_greed"]["score"]
        rating = data["fear_and_greed"]["rating"]
        return {
            "Fear_Greed_Score":  round(float(score), 1),
            "Fear_Greed_Rating": str(rating).title(),
        }
    except Exception:
        return {"Fear_Greed_Score": None, "Fear_Greed_Rating": None}


def analyze_macro_environment() -> dict:
    """
    Fetches the latest available closing price for Crude Oil, Gold, 10Y Treasury
    Yield, and the VIX from Yahoo Finance. Uses a 5-day window so the last
    available bar is always returned even on weekends or holidays.
    Also fetches the CNN Fear & Greed Index.
    """
    macro = {}
    for name, symbol in _MACRO_TICKERS.items():
        try:
            data = yf.Ticker(symbol).history(period="5d")
            if not data.empty:
                macro[name] = round(float(data["Close"].iloc[-1]), 4)
            else:
                macro[name] = None
        except Exception:
            macro[name] = None
    macro.update(_fetch_fear_greed())
    macro.update(benner_cycle_phase())
    return macro


def _macro_dashboard(macro: dict) -> None:
    """Prints a formatted macro dashboard to the terminal."""
    print("\n" + "=" * 45)
    print("       GLOBAL MACRO DASHBOARD")
    print("=" * 45)
    labels = {
        "Crude_Oil":    ("Crude Oil (WTI)",    "$/bbl"),
        "Gold":         ("Gold",               "$/oz"),
        "10Y_Treasury": ("10Y Treasury Yield", "%"),
        "VIX":          ("VIX (Fear Index)",   "pts"),
    }
    for key, (label, unit) in labels.items():
        value = macro.get(key)
        display = f"{value:.2f} {unit}" if value is not None else "N/A"
        print(f"  {label:<26} {display}")
    fg_score  = macro.get("Fear_Greed_Score")
    fg_rating = macro.get("Fear_Greed_Rating") or "N/A"
    fg_display = f"{fg_score:.0f}/100  ({fg_rating})" if fg_score is not None else "N/A"
    print(f"  {'Fear & Greed Index':<26} {fg_display}")
    benner = macro.get("Benner_Phase", "N/A")
    benner_signal = macro.get("Benner_Signal", "")
    next_boom  = macro.get("Benner_Next_Boom")
    next_panic = macro.get("Benner_Next_Panic")
    print(f"  {'Benner Cycle Phase':<26} {benner}  [{benner_signal}]")
    if next_boom:  print(f"  {'  → Next Boom (SELL)':<26} {next_boom}")
    if next_panic: print(f"  {'  → Next Panic (BUY)':<26} {next_panic}")
    print("=" * 45 + "\n")


if __name__ == "__main__":
    print("Scraping global index constituents...")
    universe = get_global_universe()
    print(f"Total unique tickers in global universe: {len(universe)}")

    print("\nFetching macro indicators...")
    macro = analyze_macro_environment()
    _macro_dashboard(macro)
