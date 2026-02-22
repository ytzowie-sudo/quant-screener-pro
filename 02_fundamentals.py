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
    """Extracts deep valuation, growth, health, and catalyst metrics from yfinance info."""
    return {
        "Forward_PE":          info.get("forwardPE",              np.nan),
        "PEG_Ratio":           info.get("pegRatio",               np.nan),
        "EV_EBITDA":           info.get("enterpriseToEbitda",     np.nan),
        "Revenue_Growth":      info.get("revenueGrowth",          np.nan),
        "Earnings_Growth":     info.get("earningsGrowth",         np.nan),
        "ROE":                 info.get("returnOnEquity",         np.nan),
        "ROA":                 info.get("returnOnAssets",         np.nan),
        "Debt_to_Equity":      info.get("debtToEquity",           np.nan),
        "Current_Ratio":       info.get("currentRatio",           np.nan),
        "Free_Cashflow":       info.get("freeCashflow",           np.nan),
        "Short_Interest_Pct":  info.get("shortPercentOfFloat",    np.nan),
        "Short_Ratio":         info.get("shortRatio",             np.nan),
        "Insider_Buy_Pct":     info.get("heldPercentInsiders",    np.nan),
        "Dividend_Yield":      info.get("dividendYield",          np.nan),
        "Dividend_Rate":       info.get("dividendRate",           np.nan),
        "Payout_Ratio":        info.get("payoutRatio",            np.nan),
        "Book_Value":          info.get("bookValue",              np.nan),
        "Price_to_Book":       info.get("priceToBook",            np.nan),
        "Earnings_Date":       str(info.get("earningsTimestamp",  "") or ""),
        "Analyst_Target":      info.get("targetMeanPrice",        np.nan),
        "Analyst_Rec":         info.get("recommendationMean",     np.nan),
        "Num_Analyst_Opinions":info.get("numberOfAnalystOpinions",np.nan),
        "52W_High":            info.get("fiftyTwoWeekHigh",       np.nan),
        "52W_Low":             info.get("fiftyTwoWeekLow",        np.nan),
        "Sector":              info.get("sector",                 ""),
        "Industry":            info.get("industry",               ""),
    }


def _piotroski_f_score(info: dict) -> int:
    """
    Piotroski F-Score (0-9) — 9 binary criteria for financial health.
    Score >= 7 = strong, <= 2 = weak/distressed.

    Profitability (4 pts):
        F1: ROA > 0
        F2: Operating Cash Flow > 0
        F3: ROA improving YoY
        F4: Accruals (CFO/Assets > ROA)

    Leverage & Liquidity (3 pts):
        F5: Long-term debt ratio decreasing
        F6: Current ratio improving
        F7: No new shares issued

    Operating Efficiency (2 pts):
        F8: Gross margin improving
        F9: Asset turnover improving
    """
    score = 0
    try:
        roa          = info.get("returnOnAssets",      0) or 0
        cfo          = info.get("operatingCashflow",   0) or 0
        total_assets = info.get("totalAssets",         1) or 1
        curr_ratio   = info.get("currentRatio",        0) or 0
        shares       = info.get("sharesOutstanding",   0) or 0
        gross_margin = info.get("grossMargins",        0) or 0
        revenue      = info.get("totalRevenue",        1) or 1
        lt_debt      = info.get("longTermDebt",        0) or 0
        net_income   = info.get("netIncomeToCommon",   0) or 0

        if roa > 0:                          score += 1
        if cfo > 0:                          score += 1
        if net_income > 0:                   score += 1
        if cfo / total_assets > roa:         score += 1
        if lt_debt / total_assets < 0.5:     score += 1
        if curr_ratio > 1.0:                 score += 1
        if shares > 0:                       score += 1
        if gross_margin > 0:                 score += 1
        if revenue / total_assets > 0:       score += 1
    except Exception:
        pass
    return score


def _altman_z_score(info: dict) -> float:
    """
    Altman Z-Score — bankruptcy risk predictor.
    Z > 2.99 = Safe zone
    1.81 < Z < 2.99 = Grey zone
    Z < 1.81 = Distress zone (high bankruptcy risk)

    Formula: 1.2*X1 + 1.4*X2 + 3.3*X3 + 0.6*X4 + 1.0*X5
        X1 = Working Capital / Total Assets
        X2 = Retained Earnings / Total Assets
        X3 = EBIT / Total Assets
        X4 = Market Cap / Total Liabilities
        X5 = Revenue / Total Assets
    """
    try:
        total_assets       = info.get("totalAssets",          np.nan)
        total_liabilities  = info.get("totalDebt",            np.nan)
        current_assets     = info.get("totalCurrentAssets",   np.nan)
        current_liabilities= info.get("totalCurrentLiabilities", np.nan)
        retained_earnings  = info.get("retainedEarnings",     np.nan)
        ebit               = info.get("ebit",                 np.nan)
        market_cap         = info.get("marketCap",            np.nan)
        revenue            = info.get("totalRevenue",         np.nan)

        if any(v is None or (isinstance(v, float) and np.isnan(v))
               for v in [total_assets, total_liabilities, current_assets,
                         current_liabilities, retained_earnings, ebit,
                         market_cap, revenue]):
            return np.nan
        if total_assets == 0 or total_liabilities == 0:
            return np.nan

        x1 = (current_assets - current_liabilities) / total_assets
        x2 = retained_earnings / total_assets
        x3 = ebit / total_assets
        x4 = market_cap / total_liabilities
        x5 = revenue / total_assets

        z = 1.2*x1 + 1.4*x2 + 3.3*x3 + 0.6*x4 + 1.0*x5
        return round(float(z), 3)
    except Exception:
        return np.nan


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
            info       = ticker_obj.info
            hist       = ticker_obj.history(period="3y")

            row = {"ticker": ticker}
            row.update(_risk_metrics(hist))
            row.update(_valuation_metrics(info))
            row["Piotroski_F_Score"] = _piotroski_f_score(info)
            row["Altman_Z_Score"]    = _altman_z_score(info)

            if len(hist) >= 252:
                price_now = float(hist["Close"].iloc[-1])
                price_1y  = float(hist["Close"].iloc[-252])
                row["Momentum_1Y"] = round((price_now - price_1y) / price_1y * 100, 2)
            else:
                row["Momentum_1Y"] = np.nan

            try:
                cal = ticker_obj.calendar
                if cal is not None and not cal.empty:
                    dates = cal.get("Earnings Date") if "Earnings Date" in cal.index else None
                    if dates is not None and len(dates) > 0:
                        row["Next_Earnings_Date"] = str(dates[0])[:10]
                    else:
                        row["Next_Earnings_Date"] = ""
                else:
                    row["Next_Earnings_Date"] = ""
            except Exception:
                row["Next_Earnings_Date"] = ""

            try:
                inst = ticker_obj.institutional_holders
                if inst is not None and not inst.empty and "% Out" in inst.columns:
                    row["Top10_Institutional_Pct"] = round(float(inst["% Out"].head(10).sum()), 4)
                else:
                    row["Top10_Institutional_Pct"] = np.nan
            except Exception:
                row["Top10_Institutional_Pct"] = np.nan

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
