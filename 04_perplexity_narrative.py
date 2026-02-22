import json
import os
import re
import time

import pandas as pd
import requests
from tqdm import tqdm

try:
    import streamlit as st
    PERPLEXITY_API_KEY = st.secrets.get("PERPLEXITY_API_KEY", "") or os.environ.get("PERPLEXITY_API_KEY", "")
except Exception:
    PERPLEXITY_API_KEY = os.environ.get("PERPLEXITY_API_KEY", "")
_API_URL   = "https://api.perplexity.ai/chat/completions"
_MODEL     = "sonar"
_TOP_N     = 15
_QUANT_W   = 0.70
_NARR_W    = 0.30

_DEFAULT_NARRATIVE = {
    "Catalysts":       "N/A",
    "Threats":         "N/A",
    "AI_Impact":       "N/A",
    "Narrative_Score": 50,
}

_HEADERS = {
    "Authorization": f"Bearer {PERPLEXITY_API_KEY}",
    "Content-Type":  "application/json",
}


def _extract_json(text: str) -> dict:
    """
    Attempts json.loads() first. Falls back to regex extraction of the
    4 required keys if the model wraps the JSON in markdown fences.
    Last resort: extracts individual fields via regex from free text.
    """
    text = text.strip()

    # 1. Direct JSON parse
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass

    # 2. JSON inside markdown fences
    fence_match = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", text, re.DOTALL)
    if fence_match:
        try:
            return json.loads(fence_match.group(1))
        except json.JSONDecodeError:
            pass

    # 3. Any JSON object in the text
    bare_match = re.search(r"\{.*\}", text, re.DOTALL)
    if bare_match:
        try:
            return json.loads(bare_match.group(0))
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
        # Try to infer score from sentiment words in the full response
        positive = len(re.findall(r'\b(bullish|strong|growth|upside|buy|catalyst|positive|momentum|beat|surge)\b', text, re.IGNORECASE))
        negative = len(re.findall(r'\b(bearish|risk|threat|decline|sell|weak|miss|drop|concern|headwind)\b', text, re.IGNORECASE))
        total = positive + negative
        if total > 0:
            result["Narrative_Score"] = int(round((positive / total) * 100))

    return result


def get_perplexity_narrative(ticker: str) -> dict:
    """
    Calls the Perplexity sonar API to generate a hedge-fund-style narrative
    for the given ticker. Returns a dict with keys:
        Catalysts, Threats, AI_Impact, Narrative_Score.
    Falls back to _DEFAULT_NARRATIVE on any error.
    """
    payload = {
        "model": _MODEL,
        "messages": [
            {
                "role": "system",
                "content": "You are a ruthless hedge fund analyst. Analyze the stock strictly.",
            },
            {
                "role": "user",
                "content": (
                    f"Search the live web for the stock ticker {ticker}. "
                    "Give me a JSON response with exactly 4 keys: "
                    "'Catalysts' (1 short sentence on upcoming positive events/patents/M&A), "
                    "'Threats' (1 short sentence on AI disruption/laws/macro risks), "
                    "'AI_Impact' (1 short sentence on how AI affects their moat), and "
                    "'Narrative_Score' (an integer from 0 to 100 representing the fundamental "
                    "news and future outlook). Return ONLY valid JSON, nothing else."
                ),
            },
        ],
    }

    try:
        response = requests.post(_API_URL, headers=_HEADERS, json=payload, timeout=30)
        response.raise_for_status()
        content = response.json()["choices"][0]["message"]["content"]
        parsed = _extract_json(content)

        return {
            "Catalysts":       str(parsed.get("Catalysts",       "N/A")),
            "Threats":         str(parsed.get("Threats",         "N/A")),
            "AI_Impact":       str(parsed.get("AI_Impact",       "N/A")),
            "Narrative_Score": int(parsed.get("Narrative_Score", 50)),
        }
    except Exception:
        return _DEFAULT_NARRATIVE.copy()


def run_narrative_analysis() -> pd.DataFrame:
    """
    Loads the top 15 stocks by Quant_Risk_Score from quant_risk_analyzed.csv,
    fetches a Perplexity narrative for each, computes the Ultimate_Conviction_Score,
    and saves to narrative_analyzed.csv.
    """
    df = pd.read_csv("sentiment.csv")
    if df.empty:
        print("Error: sentiment.csv is empty — run 04_sentiment_and_export.py first.")
        return pd.DataFrame()
    top15 = df.nlargest(_TOP_N, "Quant_Risk_Score").copy()
    top15.reset_index(drop=True, inplace=True)

    narratives = []
    for ticker in tqdm(top15["ticker"].tolist(), desc="Perplexity Narrative"):
        result = get_perplexity_narrative(ticker)
        result["ticker"] = ticker
        narratives.append(result)
        time.sleep(2)

    narr_df = pd.DataFrame(narratives)

    merged = top15.merge(narr_df, on="ticker", how="left")

    merged["Narrative_Score"] = pd.to_numeric(
        merged["Narrative_Score"], errors="coerce"
    ).fillna(50).clip(0, 100)

    merged["Ultimate_Conviction_Score"] = (
        merged["Quant_Risk_Score"] * _QUANT_W
        + merged["Narrative_Score"] * _NARR_W
    ).round(2)

    merged.sort_values("Ultimate_Conviction_Score", ascending=False, inplace=True)
    merged.reset_index(drop=True, inplace=True)

    merged.to_csv("ai_narrative.csv", index=False)
    return merged


if __name__ == "__main__":
    result = run_narrative_analysis()
    print("\n=== TOP 3 STRATEGIC BUYS ===")
    for i, row in result.head(3).iterrows():
        print(f"\n#{i + 1}  {row['ticker']}")
        print(f"  Ultimate Conviction Score : {row['Ultimate_Conviction_Score']}")
        print(f"  Catalysts                 : {row['Catalysts']}")
