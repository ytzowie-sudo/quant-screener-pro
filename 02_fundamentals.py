import time

import numpy as np
import pandas as pd
import yfinance as yf
from tqdm import tqdm

_RISK_FREE_RATE = 0.04
_TRADING_DAYS = 252


def _risk_metrics(hist: pd.DataFrame) -> dict:
    """
    Derives annualised return, volatility, Sharpe ratio, and max drawdown
    from a 3-year daily price history DataFrame.
    """
    try:
        close = hist["Close"].squeeze().dropna()
        if len(close) < 60:
            raise ValueError("Insufficient history")

        daily_returns = close.pct_change().dropna()

        ann_return = (1 + daily_returns.mean()) ** _TRADING_DAYS - 1
        ann_vol = daily_returns.std() * np.sqrt(_TRADING_DAYS)
        sharpe = (ann_return - _RISK_FREE_RATE) / ann_vol if ann_vol != 0 else np.nan

        rolling_max = close.cummax()
        drawdown = (close - rolling_max) / rolling_max
        max_drawdown = drawdown.min()

        return {
            "Ann_Return": ann_return,
            "Ann_Volatility": ann_vol,
            "Sharpe_Ratio": sharpe,
            "Max_Drawdown": max_drawdown,
        }
    except Exception:
        return {
            "Ann_Return": np.nan,
            "Ann_Volatility": np.nan,
            "Sharpe_Ratio": np.nan,
            "Max_Drawdown": np.nan,
        }


def _valuation_metrics(info: dict) -> dict:
    """Extracts deep valuation, growth, and health metrics from yfinance info."""
    return {
        "Forward_PE": info.get("forwardPE", np.nan),
        "PEG_Ratio": info.get("pegRatio", np.nan),
        "EV_EBITDA": info.get("enterpriseToEbitda", np.nan),
        "Revenue_Growth": info.get("revenueGrowth", np.nan),
        "Earnings_Growth": info.get("earningsGrowth", np.nan),
        "ROE": info.get("returnOnEquity", np.nan),
        "ROA": info.get("returnOnAssets", np.nan),
        "Debt_to_Equity": info.get("debtToEquity", np.nan),
        "Current_Ratio": info.get("currentRatio", np.nan),
        "Free_Cashflow": info.get("freeCashflow", np.nan),
    }


def _score_universe(df: pd.DataFrame) -> pd.Series:
    """
    Percentile-ranks each metric across the universe and combines them into
    a Fundamental_Score from 0 to 100.

    Stocks with missing metrics receive rank 0 for that component (na_option='bottom')
    so NaN never propagates into the total score.

    Reward HIGH (higher percentile = better):
        Sharpe_Ratio  (25 pts)
        ROE           (15 pts)
        Revenue_Growth (15 pts)
        Earnings_Growth(10 pts)
        Free_Cashflow  (10 pts)

    Reward LOW (lower percentile = better, so invert):
        PEG_Ratio     (10 pts)
        EV_EBITDA     (10 pts)
        Debt_to_Equity ( 3 pts)
        Max_Drawdown   ( 2 pts)  [already negative, so higher rank = less bad]

    Total: 100 pts
    """
    def pct(col: str, invert: bool = False) -> pd.Series:
        if col not in df.columns:
            return pd.Series(0.5, index=df.index)
        ranked = df[col].rank(pct=True, na_option="bottom")
        return (1 - ranked) if invert else ranked

    score = (
        pct("Sharpe_Ratio")           * 25
        + pct("ROE")                  * 15
        + pct("Revenue_Growth")       * 15
        + pct("Earnings_Growth")      * 10
        + pct("Free_Cashflow")        * 10
        + pct("PEG_Ratio",      True) * 10
        + pct("EV_EBITDA",      True) * 10
        + pct("Debt_to_Equity", True) * 3
        + pct("Max_Drawdown",   True) * 2
    )
    return score.round(2)


def evaluate_advanced_fundamentals() -> pd.DataFrame:
    """
    Institutional-grade fundamental & risk scoring engine.

    For every ticker in the universe:
      - Downloads 3 years of daily price history for risk metrics
      - Pulls deep valuation, growth, and profitability data from yfinance
      - Scores each stock from 0-100 using percentile ranking
      - Saves ALL scored stocks to fundamentals.csv (no cap — wide funnel)
    """
    universe = pd.read_csv("data_loaded.csv")
    if universe.empty:
        print("Error: data_loaded.csv is empty — run 01_data_loader.py first.")
        return pd.DataFrame()
    tickers = universe["ticker"].tolist()
    records = []

    for ticker in tqdm(tickers, desc="Building Fundamental Universe"):
        try:
            ticker_obj = yf.Ticker(ticker)
            info = ticker_obj.info
            hist = ticker_obj.history(period="3y")

            row = {"ticker": ticker}
            row.update(_risk_metrics(hist))
            row.update(_valuation_metrics(info))
            records.append(row)
        except Exception:
            records.append({"ticker": ticker})

        time.sleep(0.1)

    df = pd.DataFrame(records)

    numeric_cols = df.select_dtypes(include="number").columns
    df[numeric_cols] = df[numeric_cols].fillna(df[numeric_cols].median())

    df["Fundamental_Score"] = _score_universe(df)

    df.sort_values("Fundamental_Score", ascending=False, inplace=True)
    df.reset_index(drop=True, inplace=True)

    df.to_csv("fundamentals.csv", index=False)

    return df


if __name__ == "__main__":
    result = evaluate_advanced_fundamentals()
    if not result.empty:
        cols = ["ticker", "Fundamental_Score", "Sharpe_Ratio", "PEG_Ratio", "Revenue_Growth"]
        print("\n=== TOP 15 ELITE FUNDAMENTAL STOCKS ===")
        print(result[cols].head(15).to_string(index=False))
