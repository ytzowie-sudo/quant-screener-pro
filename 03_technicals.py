import time

import numpy as np
import pandas as pd
import pandas_ta as ta
import yfinance as yf
from tqdm import tqdm


def _last(series) -> float:
    """Safely return the last non-NaN scalar from a pandas Series."""
    if series is None or series.empty:
        return np.nan
    val = series.iloc[-1]
    return float(val) if not pd.isna(val) else np.nan


def _technical_score(
    close: float,
    sma50: float,
    sma200: float,
    bb_lower: float,
    bb_upper: float,
    rel_vol: float,
    stoch_k: float,
    stoch_d: float,
) -> float:
    """
    Scores a stock's technical setup out of 100 based on the latest day's data.

    Points breakdown:
        Price > SMA_50                              : 20 pts
        Price > SMA_200 (long-term uptrend)         : 20 pts
        Price near Lower BB (dip buy)               : 15 pts
          OR Price > Upper BB with RelVol > 1.2     : 15 pts  (breakout)
        Relative Volume > 1.2                       : 25 pts
        Stochastic oversold bounce (%K < 20 → 20)  : 10 pts
          OR Stochastic bullish cross (%K > %D)     : 10 pts
                                              Total : 100 pts
    """
    score = 0.0

    if not np.isnan(sma50) and close > sma50:
        score += 20

    if not np.isnan(sma200) and close > sma200:
        score += 20

    if not any(np.isnan(v) for v in [bb_lower, bb_upper]):
        near_lower = close <= bb_lower * 1.02
        breakout   = close >= bb_upper and not np.isnan(rel_vol) and rel_vol > 1.2
        if near_lower or breakout:
            score += 15

    if not np.isnan(rel_vol) and rel_vol > 1.2:
        score += 25

    if not any(np.isnan(v) for v in [stoch_k, stoch_d]):
        oversold_bounce = stoch_k < 20 and stoch_k > stoch_d
        bullish_cross   = stoch_k > stoch_d and stoch_k < 80
        if oversold_bounce or bullish_cross:
            score += 10

    return round(score, 2)


def evaluate_advanced_technicals() -> pd.DataFrame:
    """
    Technical & momentum scoring engine (V3 pipeline).

    Reads the deep-value universe from deep_valuation.csv, computes
    SMA_50/200, Bollinger Bands(20,2), ATR_14, and Relative Volume for
    each ticker, then scores each setup 0-100.
    NaN values are filled with column medians to preserve the full universe.
    Saves results to filtered_technicals.csv.
    """
    universe = pd.read_csv("deep_valuation.csv")
    if universe.empty:
        print("Error: deep_valuation.csv is empty — run 02_deep_valuation.py first.")
        return pd.DataFrame()

    tickers = universe["ticker"].tolist()

    records = []

    for ticker in tqdm(tickers, desc="Scoring Technical Setups"):
        try:
            hist = yf.Ticker(ticker).history(period="1y")
            if hist.empty or len(hist) < 30:
                continue

            close  = hist["Close"]
            high   = hist["High"]
            low    = hist["Low"]
            volume = hist["Volume"]

            sma50     = ta.sma(close, length=50)
            sma200    = ta.sma(close, length=200)
            bb_df     = ta.bbands(close, length=20, std=2)
            atr14     = ta.atr(high, low, close, length=14)
            vol_sma20 = ta.sma(volume, length=20)

            last_close  = _last(close)
            last_sma50  = _last(sma50)
            last_sma200 = _last(sma200)
            last_atr    = _last(atr14)

            bb_lower_col = [c for c in (bb_df.columns if bb_df is not None else []) if c.startswith("BBL")]
            bb_upper_col = [c for c in (bb_df.columns if bb_df is not None else []) if c.startswith("BBU")]
            last_bb_lower = _last(bb_df[bb_lower_col[0]]) if bb_lower_col else np.nan
            last_bb_upper = _last(bb_df[bb_upper_col[0]]) if bb_upper_col else np.nan

            last_vol_sma = _last(vol_sma20)
            last_volume  = float(volume.iloc[-1]) if not volume.empty else np.nan
            rel_vol = (last_volume / last_vol_sma) if (not np.isnan(last_vol_sma) and last_vol_sma != 0) else np.nan

            stoch_df = ta.stoch(high, low, close, k=14, d=3, smooth_k=3)
            last_stoch_k = np.nan
            last_stoch_d = np.nan
            if stoch_df is not None and not stoch_df.empty:
                k_cols = [c for c in stoch_df.columns if c.startswith("STOCHk")]
                d_cols = [c for c in stoch_df.columns if c.startswith("STOCHd")]
                last_stoch_k = _last(stoch_df[k_cols[0]]) if k_cols else np.nan
                last_stoch_d = _last(stoch_df[d_cols[0]]) if d_cols else np.nan

            tech_score = _technical_score(
                last_close, last_sma50, last_sma200,
                last_bb_lower, last_bb_upper, rel_vol,
                last_stoch_k, last_stoch_d,
            )

            records.append({
                "ticker":          ticker,
                "Last_Price":      round(last_close,  2),
                "SMA_50":          round(last_sma50,  2) if not np.isnan(last_sma50)  else np.nan,
                "SMA_200":         round(last_sma200, 2) if not np.isnan(last_sma200) else np.nan,
                "BB_Lower":        round(last_bb_lower, 2) if not np.isnan(last_bb_lower) else np.nan,
                "BB_Upper":        round(last_bb_upper, 2) if not np.isnan(last_bb_upper) else np.nan,
                "ATR_14":          round(last_atr, 2)      if not np.isnan(last_atr)      else np.nan,
                "Relative_Volume": round(rel_vol, 2)       if not np.isnan(rel_vol)       else np.nan,
                "Stoch_K":         round(last_stoch_k, 2)  if not np.isnan(last_stoch_k)  else np.nan,
                "Stoch_D":         round(last_stoch_d, 2)  if not np.isnan(last_stoch_d)  else np.nan,
                "Technical_Score": tech_score,
            })

        except Exception:
            continue

        time.sleep(0.1)

    if not records:
        print("No technical data could be fetched today.")
        return pd.DataFrame()

    result = pd.DataFrame(records)

    numeric_cols = result.select_dtypes(include="number").columns
    result[numeric_cols] = result[numeric_cols].fillna(result[numeric_cols].median())

    result.sort_values("Technical_Score", ascending=False, inplace=True)
    result.reset_index(drop=True, inplace=True)

    result.to_csv("technicals.csv", index=False)
    return result


if __name__ == "__main__":
    df = evaluate_advanced_technicals()
    print("\n=== TOP 10 STOCKS BY TECHNICAL SCORE ===")
    cols = ["ticker", "Technical_Score", "SMA_200", "Relative_Volume"]
    print(df[cols].head(10).to_string(index=False))
