import time

import pandas as pd
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
from tradingview_ta import TA_Handler, Interval
from transformers import pipeline

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}

_FINBERT_LABEL_MAP = {"positive": 1.0, "negative": -1.0, "neutral": 0.0}

_TV_BONUS = {"STRONG_BUY": 15, "BUY": 8, "NEUTRAL": 0, "SELL": -8, "STRONG_SELL": -15}

_REPORT_COLS = [
    "ticker",
    "Master_Alpha_Score",
    "Fundamental_Score",
    "Technical_Score",
    "Finbert_Score",
    "TV_Recommendation",
    "RSI_14",
    "Relative_Volume",
    "Forward_PE",
]

_EXCHANGES = ["NASDAQ", "NYSE", "AMEX"]


def _scrape_finviz_headlines(ticker: str) -> list[str]:
    """
    Scrapes the latest 15 Finviz headlines for a ticker.
    Returns an empty list on any failure.
    """
    url = f"https://finviz.com/quote.ashx?t={ticker}"
    try:
        response = requests.get(url, headers=_HEADERS, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "lxml")
        news_table = soup.find("table", id="news-table")
        if news_table is None:
            return []
        headlines = []
        for row in news_table.find_all("tr")[:15]:
            cells = row.find_all("td")
            if len(cells) >= 2:
                text = cells[1].get_text(strip=True)
                if text:
                    headlines.append(text[:512])
        return headlines
    except Exception as e:
        print(f"  [WARNING] Finviz fetch failed for {ticker}: {e}")
        return []


def _finbert_score(headlines: list[str], finbert) -> float:
    """
    Runs FinBERT on a list of headlines and returns an average score
    in [-1, 1]: positive=+1, neutral=0, negative=-1.
    Returns 0.0 if headlines list is empty or inference fails.
    """
    if not headlines:
        return 0.0
    try:
        results = finbert(headlines, truncation=True, max_length=512)
        scores = [_FINBERT_LABEL_MAP.get(r["label"].lower(), 0.0) for r in results]
        return sum(scores) / len(scores)
    except Exception as e:
        print(f"  [WARNING] FinBERT inference failed: {e}")
        return 0.0


def _tradingview_recommendation(ticker: str) -> str:
    """
    Fetches the daily TradingView technical recommendation for a ticker.
    Tries NASDAQ first, then NYSE, then AMEX. Returns 'UNKNOWN' on failure.
    """
    for exchange in _EXCHANGES:
        try:
            handler = TA_Handler(
                symbol=ticker,
                screener="america",
                exchange=exchange,
                interval=Interval.INTERVAL_1_DAY,
            )
            analysis = handler.get_analysis()
            return analysis.summary.get("RECOMMENDATION", "UNKNOWN")
        except Exception:
            continue
    return "UNKNOWN"


def generate_final_alpha_report() -> pd.DataFrame:
    """
    FinBERT Sentiment Scoring Engine (V3 pipeline step).

    Reads quant_risk_analyzed.csv and for each ticker:
      - Scrapes Finviz headlines and scores them with FinBERT
      - Attaches Finbert_Score in [-1, 1] (0.0 on any failure)

    Saves the merged dataframe to finbert_sentiment.csv.
    """
    df = pd.read_csv("quant_risk.csv")

    if df.empty:
        print("Error: quant_risk.csv is empty — re-run 03_quant_risk_models.py first.")
        return df

    print("Loading FinBERT Model... this may take a minute on first run")
    finbert = pipeline("sentiment-analysis", model="ProsusAI/finbert")

    finbert_scores = []

    for ticker in tqdm(df["ticker"].tolist(), desc="FinBERT Sentiment"):
        headlines = _scrape_finviz_headlines(ticker)
        finbert_scores.append(_finbert_score(headlines, finbert))
        time.sleep(1)

    df["Finbert_Score"] = finbert_scores
    df["Finbert_Score"] = pd.to_numeric(df["Finbert_Score"], errors="coerce").fillna(0.0)

    df.sort_values("Quant_Risk_Score", ascending=False, inplace=True)
    df.reset_index(drop=True, inplace=True)

    df.to_csv("sentiment.csv", index=False)

    return df


if __name__ == "__main__":
    result = generate_final_alpha_report()
    if not result.empty:
        print("\n=== TOP 10 BY QUANT RISK SCORE + FINBERT ===")
        cols = ["ticker", "Quant_Risk_Score", "Finbert_Score"]
        print(result[cols].head(10).to_string(index=False))
