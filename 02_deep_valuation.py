import time

import numpy as np
import pandas as pd
import yfinance as yf
from tqdm import tqdm

_RISK_FREE_RATE = 0.0409
_DEFAULT_GROWTH  = 0.05
_GRAHAM_MULTIPLIER = 8.5


def _intrinsic_value_graham(eps: float, growth: float, risk_free: float) -> float:
    """
    Simplified Graham Number / DCF hybrid:
        IV = EPS * (2 * g * 100 + _GRAHAM_MULTIPLIER) * (4.4 / (risk_free * 100))
    Returns np.nan if inputs are invalid.
    """
    if eps is None or np.isnan(eps) or eps <= 0:
        return np.nan
    if growth is None or np.isnan(growth):
        growth = _DEFAULT_GROWTH
    growth = max(min(growth, 0.30), 0.0)
    iv = eps * (2 * growth * 100 + _GRAHAM_MULTIPLIER) * (4.4 / (risk_free * 100))
    return iv if iv > 0 else np.nan


def _margin_of_safety(intrinsic: float, price: float) -> float:
    """Returns (IV - Price) / IV. Positive = undervalued."""
    if np.isnan(intrinsic) or intrinsic <= 0 or price is None or np.isnan(price) or price <= 0:
        return np.nan
    return (intrinsic - price) / intrinsic


def _score_universe(df: pd.DataFrame) -> pd.Series:
    """
    Percentile-based Deep Value Score (0-100).

    Reward HIGH:
        Margin_of_Safety        (35 pts)
        Institutional_Ownership (20 pts)
        Insider_Ownership       (15 pts)
        Free_Cashflow           (15 pts)
        ROE                     (15 pts)

    Reward LOW:
        Debt_to_Equity          (subtracted, up to -0 pts — handled via inversion)

    Total: 100 pts
    """
    def pct(col: str, invert: bool = False) -> pd.Series:
        if col not in df.columns:
            return pd.Series(0.5, index=df.index)
        ranked = df[col].rank(pct=True, na_option="bottom")
        return (1 - ranked) if invert else ranked

    score = (
        pct("Margin_of_Safety")          * 35
        + pct("Institutional_Ownership") * 20
        + pct("Insider_Ownership")       * 10
        + pct("Free_Cashflow")           * 15
        + pct("ROE")                     * 10
        + pct("Debt_to_Equity", True)    * 10
    )
    return score.round(2)


def run_deep_valuation() -> pd.DataFrame:
    """
    Iterates over the global universe, computes intrinsic value via the
    simplified Graham formula, calculates Margin of Safety, extracts
    ownership and quality metrics, scores each stock 0-100, and saves
    the result to deep_valuation.csv.
    """
    universe = pd.read_csv("fundamentals.csv")
    if universe.empty:
        print("Error: fundamentals.csv is empty — run 02_fundamentals.py first.")
        return pd.DataFrame()
    tickers = universe["ticker"].tolist()

    records = []

    for ticker in tqdm(tickers, desc="Deep Valuation Scan"):
        row = {"ticker": ticker}
        try:
            info = yf.Ticker(ticker).info

            eps          = info.get("trailingEps",        np.nan)
            price        = info.get("currentPrice",       np.nan) or info.get("regularMarketPrice", np.nan)
            growth       = info.get("earningsGrowth",     _DEFAULT_GROWTH)
            fcf          = info.get("freeCashflow",       np.nan)
            roe          = info.get("returnOnEquity",     np.nan)
            d2e          = info.get("debtToEquity",       np.nan)
            insiders     = info.get("heldPercentInsiders",      np.nan)
            institutions = info.get("heldPercentInstitutions",  np.nan)

            iv  = _intrinsic_value_graham(eps, growth, _RISK_FREE_RATE)
            mos = _margin_of_safety(iv, price)

            row.update({
                "Current_Price":           price,
                "EPS":                     eps,
                "Growth_Rate":             growth,
                "Intrinsic_Value":         round(iv,  2) if not np.isnan(iv)  else np.nan,
                "Margin_of_Safety":        round(mos, 4) if not np.isnan(mos) else np.nan,
                "Insider_Ownership":       insiders,
                "Institutional_Ownership": institutions,
                "Free_Cashflow":           fcf,
                "ROE":                     roe,
                "Debt_to_Equity":          d2e,
            })
        except Exception:
            pass

        records.append(row)
        time.sleep(0.1)

    df = pd.DataFrame(records)

    numeric_cols = df.select_dtypes(include="number").columns
    df[numeric_cols] = df[numeric_cols].fillna(df[numeric_cols].median())

    df["Deep_Value_Score"] = _score_universe(df)

    df.sort_values("Deep_Value_Score", ascending=False, inplace=True)
    df.reset_index(drop=True, inplace=True)

    df.to_csv("deep_valuation.csv", index=False)
    return df


if __name__ == "__main__":
    result = run_deep_valuation()
    print("\n=== TOP 10 DEEP VALUE STOCKS ===")
    cols = ["ticker", "Deep_Value_Score", "Margin_of_Safety", "Insider_Ownership"]
    print(result[cols].head(10).to_string(index=False))
