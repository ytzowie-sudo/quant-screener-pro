import json
import os
import re
import time

import pandas as pd
import yfinance as yf
from tqdm import tqdm

try:
    import streamlit as st
    PERPLEXITY_API_KEY = st.secrets.get("PERPLEXITY_API_KEY", "") or os.environ.get("PERPLEXITY_API_KEY", "")
except Exception:
    PERPLEXITY_API_KEY = os.environ.get("PERPLEXITY_API_KEY", "")

import requests

_API_URL  = "https://api.perplexity.ai/chat/completions"
_MODEL    = "sonar"
_TOP_N    = 50
_EVENT_N  = 5

_HEADERS = {
    "Authorization": f"Bearer {PERPLEXITY_API_KEY}",
    "Content-Type":  "application/json",
}


def _extract_json(text: str) -> dict:
    text = text.strip()

    # 1. Direct JSON parse
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass

    # 2. JSON inside markdown fences
    fence = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", text, re.DOTALL)
    if fence:
        try:
            return json.loads(fence.group(1))
        except json.JSONDecodeError:
            pass

    # 3. Any JSON object in the text
    bare = re.search(r"\{.*\}", text, re.DOTALL)
    if bare:
        try:
            return json.loads(bare.group(0))
        except json.JSONDecodeError:
            pass

    # 4. Last resort: extract individual fields via regex from free text
    result = {}
    for key in ["Catalysts", "Threats", "AI_Impact"]:
        m = re.search(
            rf'["\']?{key}["\']?\s*[:=]\s*["\']?([^\'"\n{{}}]+)["\']?',
            text, re.IGNORECASE
        )
        if m:
            result[key] = m.group(1).strip().rstrip(",")

    score_m = re.search(
        r'["\']?Narrative_Score["\']?\s*[:=]\s*(\d{1,3})',
        text, re.IGNORECASE
    )
    if score_m:
        result["Narrative_Score"] = int(score_m.group(1))
    elif not result:
        positive = len(re.findall(r'\b(bullish|strong|growth|upside|buy|catalyst|positive|momentum|beat|surge)\b', text, re.IGNORECASE))
        negative = len(re.findall(r'\b(bearish|risk|threat|decline|sell|weak|miss|drop|concern|headwind)\b', text, re.IGNORECASE))
        total = positive + negative
        if total > 0:
            result["Narrative_Score"] = int(round((positive / total) * 100))

    return result


def _momentum_candidates(n: int = _TOP_N) -> list[str]:
    """
    Selects top N tickers by short-term momentum signal from fundamentals.csv:
    - High Relative Volume (unusual activity today)
    - High Short Interest (short squeeze potential)
    - Earnings within 7 days (imminent catalyst)
    Falls back to top N by Momentum_1Y if columns are missing.
    """
    try:
        df = pd.read_csv("fundamentals.csv")
    except FileNotFoundError:
        try:
            df = pd.read_csv("data_loaded.csv")
        except FileNotFoundError:
            return []

    if df.empty:
        return []

    score = pd.Series(0.0, index=df.index)

    if "Short_Interest_Pct" in df.columns:
        score += df["Short_Interest_Pct"].rank(pct=True, na_option="bottom") * 30

    if "Momentum_1Y" in df.columns:
        score += df["Momentum_1Y"].rank(pct=True, na_option="bottom") * 40

    if "Earnings_Growth" in df.columns:
        score += df["Earnings_Growth"].rank(pct=True, na_option="bottom") * 30

    df["_event_score"] = score
    top = df.nlargest(n, "_event_score")["ticker"].tolist()
    return top


def _get_event_narrative(ticker: str) -> dict:
    """
    Calls Perplexity with an event-driven prompt focused on TODAY's news,
    upcoming catalysts, and short-term price triggers.
    """
    payload = {
        "model": _MODEL,
        "messages": [
            {
                "role": "system",
                "content": (
                    "You are an event-driven hedge fund trader. "
                    "Focus exclusively on short-term catalysts (1-30 days): "
                    "earnings surprises, M&A, FDA approvals, analyst upgrades, "
                    "short squeezes, macro events. Be ruthlessly concise."
                ),
            },
            {
                "role": "user",
                "content": (
                    f"Search live web for {ticker} RIGHT NOW. "
                    "What is happening TODAY or in the next 30 days that could move this stock? "
                    "Return ONLY valid JSON with exactly 4 keys: "
                    "'Catalysts' (the specific near-term event/trigger), "
                    "'Threats' (what could kill this trade), "
                    "'AI_Impact' (AI disruption risk or opportunity), "
                    "'Narrative_Score' (0-100, how strong is the short-term setup). "
                    "Return ONLY valid JSON, nothing else."
                ),
            },
        ],
    }

    try:
        resp = requests.post(_API_URL, headers=_HEADERS, json=payload, timeout=30)
        resp.raise_for_status()
        content = resp.json()["choices"][0]["message"]["content"]
        parsed = _extract_json(content)
        return {
            "Catalysts":       str(parsed.get("Catalysts",       "N/A")),
            "Threats":         str(parsed.get("Threats",         "N/A")),
            "AI_Impact":       str(parsed.get("AI_Impact",       "N/A")),
            "Narrative_Score": int(parsed.get("Narrative_Score", 50)),
        }
    except Exception:
        return {"Catalysts": "N/A", "Threats": "N/A", "AI_Impact": "N/A", "Narrative_Score": 50}


def run_event_driven_analysis() -> pd.DataFrame:
    """
    Option B — Perplexity-first parallel track for Court Terme.

    1. Selects top 50 tickers by momentum/short-interest signal (independent of quant scores)
    2. Calls Perplexity with an event-driven prompt for each
    3. Ranks by Narrative_Score
    4. Saves top results to event_driven.csv
    """
    candidates = _momentum_candidates(_TOP_N)
    if not candidates:
        print("Error: no candidates found — run 02_fundamentals.py first.")
        return pd.DataFrame()

    print(f"  Event-driven track: analysing top {len(candidates)} momentum candidates via Perplexity...")

    records = []
    for ticker in tqdm(candidates, desc="Event-Driven Perplexity"):
        narrative = _get_event_narrative(ticker)
        narrative["ticker"] = ticker
        records.append(narrative)
        time.sleep(2)

    df = pd.DataFrame(records)
    df.sort_values("Narrative_Score", ascending=False, inplace=True)
    df.reset_index(drop=True, inplace=True)

    try:
        fund_df = pd.read_csv("fundamentals.csv")
        keep = [c for c in ["ticker", "Short_Interest_Pct", "Short_Ratio",
                             "Momentum_1Y", "Next_Earnings_Date", "Sector",
                             "Analyst_Target", "Analyst_Rec"] if c in fund_df.columns]
        df = df.merge(fund_df[keep], on="ticker", how="left")
    except Exception:
        pass

    df["Event_Driven"] = True

    df.to_csv("event_driven.csv", index=False)
    print(f"  ✔  event_driven.csv saved — top {_EVENT_N} event plays:")
    for _, row in df.head(_EVENT_N).iterrows():
        print(f"     {row['ticker']:8s}  Narrative={row['Narrative_Score']}  {row['Catalysts'][:60]}")

    return df


if __name__ == "__main__":
    run_event_driven_analysis()
