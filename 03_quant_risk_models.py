import time

import numpy as np
import pandas as pd
import yfinance as yf
from tqdm import tqdm

try:
    from hurst import compute_Hc
    _HURST_AVAILABLE = True
except ImportError:
    _HURST_AVAILABLE = False

try:
    from tradingview_ta import TA_Handler, Interval
    _TV_AVAILABLE = True
except ImportError:
    _TV_AVAILABLE = False

_TRADING_DAYS   = 252
_MC_PATHS       = 1000
_VAR_PERCENTILE = 5
_TOP_N          = 100
_STOCH_PERIOD   = 14
_STOCH_SMOOTH   = 3

_TV_EXCHANGES   = ["NASDAQ", "NYSE", "AMEX"]
_TV_BONUS_MAP   = {"STRONG_BUY": 15, "BUY": 8, "NEUTRAL": 0, "SELL": -5, "STRONG_SELL": -10}


# ---------------------------------------------------------------------------
# Macro trend helpers
# ---------------------------------------------------------------------------

def _commodity_trend(symbol: str, period: str = "3mo") -> str:
    """Returns 'up', 'down', or 'flat' based on first vs last close over period."""
    try:
        hist = yf.Ticker(symbol).history(period=period)
        if hist.empty or len(hist) < 5:
            return "flat"
        first = float(hist["Close"].iloc[0])
        last  = float(hist["Close"].iloc[-1])
        change = (last - first) / first
        if change > 0.03:
            return "up"
        if change < -0.03:
            return "down"
        return "flat"
    except Exception:
        return "flat"


def _get_macro_trends() -> dict:
    """Fetches 3-month trend for Crude Oil and Gold."""
    print("  Fetching macro commodity trends...")
    return {
        "oil":  _commodity_trend("CL=F"),
        "gold": _commodity_trend("GC=F"),
    }


# ---------------------------------------------------------------------------
# Hurst exponent
# ---------------------------------------------------------------------------

def _hurst_exponent(close: pd.Series) -> float:
    """
    Calculates the Hurst exponent using the hurst library if available,
    otherwise falls back to a basic R/S analysis implementation.
    H > 0.5 → trending/persistent
    H < 0.5 → mean-reverting
    H ≈ 0.5 → random walk
    """
    prices = close.dropna().values
    if len(prices) < 20:
        return np.nan

    if _HURST_AVAILABLE:
        try:
            H, _, _ = compute_Hc(prices, kind="price", simplified=True)
            return round(float(H), 4)
        except Exception:
            pass

    try:
        lags = range(2, min(20, len(prices) // 2))
        tau  = [np.std(np.subtract(prices[lag:], prices[:-lag])) for lag in lags]
        if len(tau) < 2 or np.std(tau) == 0:
            return np.nan
        poly = np.polyfit(np.log(list(lags)), np.log(tau), 1)
        return round(float(poly[0]), 4)
    except Exception:
        return np.nan


# ---------------------------------------------------------------------------
# VWAP
# ---------------------------------------------------------------------------

def _vwap(df: pd.DataFrame) -> float:
    """Calculates VWAP using cumulative method; returns the last value."""
    typical = (df["High"] + df["Low"] + df["Close"]) / 3
    cumvol  = df["Volume"].cumsum()
    if cumvol.iloc[-1] == 0:
        return np.nan
    vwap_series = (typical * df["Volume"]).cumsum() / cumvol
    return float(vwap_series.iloc[-1])


# ---------------------------------------------------------------------------
# Monte Carlo VaR
# ---------------------------------------------------------------------------

def _monte_carlo_var(log_returns: pd.Series) -> float:
    """
    Runs a Monte Carlo simulation (1000 paths × 252 days).
    Returns the 5th-percentile terminal loss as a positive fraction.
    """
    mu  = log_returns.mean()
    sig = log_returns.std()
    if np.isnan(mu) or np.isnan(sig) or sig == 0:
        return 0.0
    rng = np.random.default_rng(seed=42)
    shocks = rng.normal(mu, sig, (_MC_PATHS, _TRADING_DAYS))
    terminal_returns = np.exp(shocks.sum(axis=1)) - 1
    return abs(float(np.percentile(terminal_returns, _VAR_PERCENTILE)))


# ---------------------------------------------------------------------------
# Stochastic Oscillator + Bullish Divergence
# ---------------------------------------------------------------------------

def _stochastic(df: pd.DataFrame, k_period: int = 14, d_smooth: int = 3) -> tuple[pd.Series, pd.Series]:
    """Returns (%K, %D) stochastic oscillator series."""
    low_min  = df["Low"].rolling(k_period).min()
    high_max = df["High"].rolling(k_period).max()
    denom = high_max - low_min
    denom = denom.replace(0, np.nan)
    k = 100 * (df["Close"] - low_min) / denom
    d = k.rolling(d_smooth).mean()
    return k, d


def _bullish_divergence(close: pd.Series, stoch_k: pd.Series, window: int = 14) -> bool:
    """
    Returns True if price made a lower low over the last `window` bars
    while Stochastic %K made a higher low — classic bullish divergence.
    """
    try:
        price_window = close.iloc[-window:]
        stoch_window = stoch_k.iloc[-window:]
        if price_window.isna().all() or stoch_window.isna().all():
            return False
        price_min_idx = price_window.idxmin()
        stoch_min_idx = stoch_window.idxmin()
        price_earlier_low = float(price_window.iloc[0])
        stoch_earlier_low = float(stoch_window.iloc[0])
        price_recent_low  = float(price_window[price_min_idx])
        stoch_recent_low  = float(stoch_window[stoch_min_idx])
        return (price_recent_low < price_earlier_low) and (stoch_recent_low > stoch_earlier_low)
    except Exception:
        return False


# ---------------------------------------------------------------------------
# TradingView recommendation
# ---------------------------------------------------------------------------

def _tradingview_rec(ticker: str) -> str:
    """Fetches the 1-day TradingView recommendation. Returns 'N/A' on failure."""
    if not _TV_AVAILABLE:
        return "N/A"
    for exchange in _TV_EXCHANGES:
        try:
            handler = TA_Handler(
                symbol=ticker,
                screener="america",
                exchange=exchange,
                interval=Interval.INTERVAL_1_DAY,
            )
            return handler.get_analysis().summary["RECOMMENDATION"]
        except Exception:
            continue
    return "N/A"


# ---------------------------------------------------------------------------
# Commodity correlation bonus/malus
# ---------------------------------------------------------------------------

def _commodity_adjustment(sector: str, macro: dict) -> int:
    """
    Returns a score adjustment based on sector and commodity trend:
      Energy + Oil up    → +8
      Energy + Oil down  → -5
      Industrials/Transport + Oil heavily up → -5
    """
    if not sector:
        return 0
    sector_lower = sector.lower()
    oil_trend = macro.get("oil", "flat")

    if "energy" in sector_lower:
        if oil_trend == "up":
            return 8
        if oil_trend == "down":
            return -5

    if any(k in sector_lower for k in ["industrial", "transport", "airline"]):
        if oil_trend == "up":
            return -5

    return 0


# ---------------------------------------------------------------------------
# Per-ticker computation
# ---------------------------------------------------------------------------

_SPY_RETURNS_CACHE: pd.Series | None = None

def _get_spy_returns() -> pd.Series:
    """Fetches SPY 1Y daily returns once and caches them."""
    global _SPY_RETURNS_CACHE
    if _SPY_RETURNS_CACHE is None:
        try:
            spy = yf.Ticker("SPY").history(period="1y")["Close"]
            _SPY_RETURNS_CACHE = spy.pct_change().dropna()
        except Exception:
            _SPY_RETURNS_CACHE = pd.Series(dtype=float)
    return _SPY_RETURNS_CACHE


def _beta(stock_returns: pd.Series) -> float:
    """Calculates Beta vs SPY. Beta > 1 = more volatile than market."""
    try:
        spy_ret = _get_spy_returns()
        aligned = pd.concat([stock_returns, spy_ret], axis=1).dropna()
        if len(aligned) < 30:
            return np.nan
        cov = aligned.cov().iloc[0, 1]
        var = aligned.iloc[:, 1].var()
        return round(cov / var, 3) if var != 0 else np.nan
    except Exception:
        return np.nan


def _compute_metrics(ticker: str, macro: dict) -> dict:
    """Downloads 1y of OHLCV data and computes all quant metrics."""
    base = {
        "VWAP":               np.nan,
        "Last_Price":         np.nan,
        "Price_vs_VWAP":      np.nan,
        "VaR_95":             np.nan,
        "Ann_Volatility":     np.nan,
        "Hurst_Exponent":     np.nan,
        "Beta":               np.nan,
        "Stoch_K":            np.nan,
        "Stoch_D":            np.nan,
        "Bullish_Divergence": False,
        "Sector":             "Unknown",
        "Commodity_Adj":      0,
        "TradingView_Rec":    "N/A",
    }
    try:
        t    = yf.Ticker(ticker)
        hist = t.history(period="1y")
        if hist.empty or len(hist) < 30:
            return base

        close      = hist["Close"]
        last_price = float(close.iloc[-1])

        vwap = _vwap(hist)
        price_vs_vwap = (last_price - vwap) / vwap if (not np.isnan(vwap) and vwap != 0) else np.nan

        daily_ret = close.pct_change().dropna()
        log_ret   = np.log(close / close.shift(1)).dropna()

        var_95  = 0.0 if (daily_ret.std() == 0 or np.isnan(daily_ret.std())) else _monte_carlo_var(log_ret)
        ann_vol = float(log_ret.std() * np.sqrt(_TRADING_DAYS))

        hurst = _hurst_exponent(close)

        stoch_k, stoch_d = _stochastic(hist, _STOCH_PERIOD, _STOCH_SMOOTH)
        divergence = _bullish_divergence(close, stoch_k)

        sector = ""
        try:
            sector = t.info.get("sector", "") or ""
        except Exception:
            pass

        commodity_adj = _commodity_adjustment(sector, macro)
        tv_rec        = _tradingview_rec(ticker)

        beta = _beta(daily_ret)

        return {
            "VWAP":               round(vwap,           4),
            "Last_Price":         round(last_price,      2),
            "Price_vs_VWAP":      round(price_vs_vwap,   4) if not np.isnan(price_vs_vwap) else np.nan,
            "VaR_95":             round(var_95,           4),
            "Ann_Volatility":     round(ann_vol,          4),
            "Hurst_Exponent":     hurst,
            "Beta":               beta,
            "Stoch_K":            round(float(stoch_k.iloc[-1]), 2) if not np.isnan(stoch_k.iloc[-1]) else np.nan,
            "Stoch_D":            round(float(stoch_d.iloc[-1]), 2) if not np.isnan(stoch_d.iloc[-1]) else np.nan,
            "Bullish_Divergence": divergence,
            "Sector":             sector,
            "Commodity_Adj":      commodity_adj,
            "TradingView_Rec":    tv_rec,
        }
    except Exception:
        return base


# ---------------------------------------------------------------------------
# Scoring
# ---------------------------------------------------------------------------

def _score_universe(df: pd.DataFrame) -> pd.Series:
    """
    Ultimate Quant Risk Score (0-100) + bonuses.

    Percentile components (base 100):
        VaR_95          (25 pts) — reward LOW
        Ann_Volatility  (20 pts) — reward LOW
        Hurst_Exponent  (25 pts) — reward HIGH (H > 0.5 = trending)
        VWAP proximity  (20 pts) — reward ~5% above VWAP

    Discrete bonuses (applied after, then clipped to 100):
        Bullish_Divergence = True  → +10
        TradingView STRONG_BUY     → +15, BUY → +8, etc.
        Commodity_Adj              → ±5 to ±8
    """
    def pct(col: str, invert: bool = False) -> pd.Series:
        ranked = df[col].rank(pct=True, na_option="bottom")
        return (1 - ranked) if invert else ranked

    vwap_proximity = (1 - ((df["Price_vs_VWAP"] - 0.05).abs() / 0.50)).clip(0, 1)

    base_score = (
        pct("VaR_95",         invert=True) * 25
        + pct("Ann_Volatility", invert=True) * 20
        + pct("Hurst_Exponent")              * 25
        + vwap_proximity                     * 20
    )

    divergence_bonus = df["Bullish_Divergence"].astype(int) * 10
    tv_bonus         = df["TradingView_Rec"].map(_TV_BONUS_MAP).fillna(0)
    commodity_bonus  = df["Commodity_Adj"]

    total = (base_score + divergence_bonus + tv_bonus + commodity_bonus).clip(0, 100)
    return total.round(2)


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

def run_quant_models() -> pd.DataFrame:
    """
    Loads top 100 stocks from technicals.csv, fetches macro trends, runs all
    quant models per ticker, scores, merges with technical data, and saves to
    quant_risk.csv.
    """
    technicals = pd.read_csv("technicals.csv")
    if technicals.empty:
        print("Error: technicals.csv is empty — run 03_technicals.py first.")
        return pd.DataFrame()
    top100  = technicals.nlargest(_TOP_N, "Technical_Score").copy()
    tickers = top100["ticker"].tolist()

    macro = _get_macro_trends()
    print(f"  Oil trend: {macro['oil']} | Gold trend: {macro['gold']}")

    records = []
    for ticker in tqdm(tickers, desc="Ultimate Quant Lab"):
        row = {"ticker": ticker}
        row.update(_compute_metrics(ticker, macro))
        records.append(row)
        time.sleep(0.1)

    risk_df = pd.DataFrame(records)

    numeric_cols = risk_df.select_dtypes(include="number").columns
    risk_df[numeric_cols] = risk_df[numeric_cols].fillna(risk_df[numeric_cols].median())

    risk_df["Quant_Risk_Score"] = _score_universe(risk_df)

    export_cols = [
        "ticker", "VWAP", "Last_Price", "Price_vs_VWAP", "VaR_95",
        "Ann_Volatility", "Hurst_Exponent", "Beta", "Stoch_K", "Stoch_D",
        "Bullish_Divergence", "Sector", "Commodity_Adj",
        "TradingView_Rec", "Quant_Risk_Score",
    ]

    merged = top100.merge(risk_df[export_cols], on="ticker", how="left")
    merged.sort_values("Quant_Risk_Score", ascending=False, inplace=True)
    merged.reset_index(drop=True, inplace=True)

    merged.to_csv("quant_risk.csv", index=False)
    return merged


if __name__ == "__main__":
    result = run_quant_models()
    print("\n=== TOP 10 INSTITUTIONAL QUANT BETS ===")
    cols = ["ticker", "Quant_Risk_Score", "Hurst_Exponent", "Bullish_Divergence", "TradingView_Rec"]
    print(result[cols].head(10).to_string(index=False))
