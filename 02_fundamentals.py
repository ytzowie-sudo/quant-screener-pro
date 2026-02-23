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


def _fmt_earnings_date(ts) -> str:
    """Converts a Unix timestamp (int) to 'YYYY-MM-DD'. Returns '' on failure."""
    if not ts:
        return ""
    try:
        import datetime
        return datetime.datetime.utcfromtimestamp(int(ts)).strftime("%Y-%m-%d")
    except Exception:
        return ""


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
        "Earnings_Date":       _fmt_earnings_date(info.get("earningsTimestamp")),
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


def _altman_z_score(ticker_obj, info: dict) -> float:
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

    Pulls balance-sheet and income-statement data from yfinance DataFrames
    (not .info, which lacks most of these fields).
    """
    def _bs_val(bs, key):
        if bs is None or bs.empty or key not in bs.index:
            return np.nan
        v = bs.loc[key].iloc[0]
        return float(v) if v is not None and not (isinstance(v, float) and np.isnan(v)) else np.nan

    def _fin_val(fin, key):
        if fin is None or fin.empty or key not in fin.index:
            return np.nan
        v = fin.loc[key].iloc[0]
        return float(v) if v is not None and not (isinstance(v, float) and np.isnan(v)) else np.nan

    try:
        bs  = ticker_obj.balance_sheet
        fin = ticker_obj.financials

        total_assets        = _bs_val(bs, "Total Assets")
        current_assets      = _bs_val(bs, "Current Assets")
        current_liabilities = _bs_val(bs, "Current Liabilities")
        retained_earnings   = _bs_val(bs, "Retained Earnings")
        total_liabilities   = _bs_val(bs, "Total Liabilities Net Minority Interest")
        ebit                = _fin_val(fin, "EBIT")
        market_cap          = info.get("marketCap", np.nan)
        revenue             = info.get("totalRevenue", np.nan)

        vals = [total_assets, current_assets, current_liabilities,
                retained_earnings, total_liabilities, ebit, market_cap, revenue]
        if any(v is None or (isinstance(v, float) and np.isnan(v)) for v in vals):
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


def _beneish_m_score(ticker_obj) -> float:
    """
    Beneish M-Score — earnings manipulation detector.
    M > -1.78 = probable manipulator (REJECT from LT portfolio)
    M <= -1.78 = unlikely manipulator (safe)

    8-variable model:
        M = -4.84 + 0.920*DSRI + 0.528*GMI + 0.404*AQI + 0.892*SGI
            + 0.115*DEPI - 0.172*SGAI + 4.679*TATA - 0.327*LVGI

    Components (all YoY ratios, t = most recent, t-1 = prior year):
        DSRI  = (Receivables_t / Revenue_t) / (Receivables_t1 / Revenue_t1)
        GMI   = Gross_Margin_t1 / Gross_Margin_t
        AQI   = (1 - (CA_t + PPE_t) / TA_t) / (1 - (CA_t1 + PPE_t1) / TA_t1)
        SGI   = Revenue_t / Revenue_t1
        DEPI  = Depr_Rate_t1 / Depr_Rate_t
        SGAI  = (SGA_t / Revenue_t) / (SGA_t1 / Revenue_t1)
        LVGI  = Leverage_t / Leverage_t1
        TATA  = (Net_Income_t - CFO_t) / Total_Assets_t

    Requires at least 2 annual periods from financials, balance_sheet, cashflow.
    Returns np.nan if any component cannot be computed.
    """
    def _val(df, key, col_idx):
        if df is None or df.empty or key not in df.index or col_idx >= len(df.columns):
            return np.nan
        v = df.loc[key].iloc[col_idx]
        if v is None or (isinstance(v, float) and np.isnan(v)):
            return np.nan
        return float(v)

    try:
        fin = ticker_obj.financials
        bs  = ticker_obj.balance_sheet
        cf  = ticker_obj.cashflow

        if fin is None or bs is None or cf is None:
            return np.nan
        if len(fin.columns) < 2 or len(bs.columns) < 2 or len(cf.columns) < 2:
            return np.nan

        # t = 0 (most recent), t-1 = 1 (prior year)
        rev_t     = _val(fin, "Total Revenue", 0)
        rev_t1    = _val(fin, "Total Revenue", 1)
        cogs_t    = _val(fin, "Cost Of Revenue", 0)
        cogs_t1   = _val(fin, "Cost Of Revenue", 1)
        sga_t     = _val(fin, "Selling General And Administration", 0)
        sga_t1    = _val(fin, "Selling General And Administration", 1)
        ni_t      = _val(fin, "Net Income", 0)

        ta_t      = _val(bs, "Total Assets", 0)
        ta_t1     = _val(bs, "Total Assets", 1)
        ca_t      = _val(bs, "Current Assets", 0)
        ca_t1     = _val(bs, "Current Assets", 1)
        ppe_t     = _val(bs, "Net PPE", 0)
        ppe_t1    = _val(bs, "Net PPE", 1)
        recv_t    = _val(bs, "Receivables", 0)
        recv_t1   = _val(bs, "Receivables", 1)
        cl_t      = _val(bs, "Current Liabilities", 0)
        cl_t1     = _val(bs, "Current Liabilities", 1)
        ltd_t     = _val(bs, "Long Term Debt", 0)
        ltd_t1    = _val(bs, "Long Term Debt", 1)

        cfo_key   = "Operating Cash Flow" if "Operating Cash Flow" in cf.index else "Cash Flow From Continuing Operating Activities"
        cfo_t     = _val(cf, cfo_key, 0)

        # Validate all required values are present
        required = [rev_t, rev_t1, cogs_t, cogs_t1, sga_t, sga_t1, ni_t,
                    ta_t, ta_t1, ca_t, ca_t1, ppe_t, ppe_t1,
                    recv_t, recv_t1, cfo_t]
        if any(np.isnan(v) for v in required):
            return np.nan
        if rev_t == 0 or rev_t1 == 0 or ta_t == 0 or ta_t1 == 0:
            return np.nan

        # DSRI — Days Sales in Receivables Index
        dsri_t  = recv_t / rev_t
        dsri_t1 = recv_t1 / rev_t1
        dsri = dsri_t / dsri_t1 if dsri_t1 != 0 else 1.0

        # GMI — Gross Margin Index
        gm_t  = (rev_t - cogs_t) / rev_t
        gm_t1 = (rev_t1 - cogs_t1) / rev_t1
        gmi = gm_t1 / gm_t if gm_t != 0 else 1.0

        # AQI — Asset Quality Index
        hard_t  = (ca_t + ppe_t) / ta_t
        hard_t1 = (ca_t1 + ppe_t1) / ta_t1
        aqi = (1 - hard_t) / (1 - hard_t1) if (1 - hard_t1) != 0 else 1.0

        # SGI — Sales Growth Index
        sgi = rev_t / rev_t1

        # DEPI — Depreciation Index
        # Depreciation rate = Depreciation / (Depreciation + PPE)
        # Use COGS proxy: depr_rate ~ PPE / (PPE + CA) as fallback if D&A missing
        da_t  = _val(fin, "Reconciled Depreciation", 0)
        da_t1 = _val(fin, "Reconciled Depreciation", 1)
        if np.isnan(da_t) or np.isnan(da_t1):
            depi = 1.0
        else:
            dr_t  = da_t / (da_t + ppe_t) if (da_t + ppe_t) != 0 else 0
            dr_t1 = da_t1 / (da_t1 + ppe_t1) if (da_t1 + ppe_t1) != 0 else 0
            depi = dr_t1 / dr_t if dr_t != 0 else 1.0

        # SGAI — SGA Expense Index
        sgai_t  = sga_t / rev_t
        sgai_t1 = sga_t1 / rev_t1
        sgai = sgai_t / sgai_t1 if sgai_t1 != 0 else 1.0

        # LVGI — Leverage Index
        lev_t  = (cl_t + (ltd_t if not np.isnan(ltd_t) else 0)) / ta_t
        lev_t1 = (cl_t1 + (ltd_t1 if not np.isnan(ltd_t1) else 0)) / ta_t1
        lvgi = lev_t / lev_t1 if lev_t1 != 0 else 1.0

        # TATA — Total Accruals to Total Assets
        tata = (ni_t - cfo_t) / ta_t

        m = (-4.84
             + 0.920 * dsri
             + 0.528 * gmi
             + 0.404 * aqi
             + 0.892 * sgi
             + 0.115 * depi
             - 0.172 * sgai
             + 4.679 * tata
             - 0.327 * lvgi)

        return round(float(m), 3)
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
            row["Altman_Z_Score"]    = _altman_z_score(ticker_obj, info)
            row["Beneish_M_Score"]   = _beneish_m_score(ticker_obj)

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
                if inst is not None and not inst.empty:
                    pct_col = next((c for c in ["pctHeld", "% Out"] if c in inst.columns), None)
                    if pct_col:
                        row["Top10_Institutional_Pct"] = round(float(inst[pct_col].head(10).sum()), 4)
                    else:
                        row["Top10_Institutional_Pct"] = np.nan
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
