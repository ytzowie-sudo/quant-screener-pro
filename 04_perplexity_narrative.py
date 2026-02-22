import json
import os
import re
import time

import pandas as pd
import requests
from tqdm import tqdm

from _secrets_helper import get_secret
PERPLEXITY_API_KEY = get_secret("PERPLEXITY_API_KEY")
if not PERPLEXITY_API_KEY:
    print("  [WARNING] PERPLEXITY_API_KEY not set — narratives will use defaults (score=50, N/A fields).")
    print("  → Set it in .streamlit/secrets.toml: PERPLEXITY_API_KEY = \"pplx-xxxx\"")
_API_URL   = "https://api.perplexity.ai/chat/completions"
_MODEL     = "sonar"
_TOP_N     = 30   # Expanded to 30 to feed 3 distinct pools of 10 each
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

# Score weights for Ultimate_Conviction_Score
# All inputs normalized to 0-100 before weighting
_W_QUANT     = 0.35   # Quant Risk (Hurst, VaR, VWAP, divergence)
_W_NARR      = 0.25   # Perplexity AI narrative
_W_FUND      = 0.20   # Fundamental (Sharpe, ROE, PEG, FCF...)
_W_FINBERT   = 0.10   # FinBERT news sentiment
_W_DEEPVAL   = 0.10   # Deep Value (Graham, MoS, ownership)


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
                    f"Analyze stock ticker {ticker} using live web data. "
                    "You MUST respond with ONLY a raw JSON object — no markdown, no explanation, no code fences. "
                    "The JSON must have exactly these 4 keys:\n"
                    '{ "Catalysts": "one sentence on upcoming catalysts (earnings, M&A, product launch)", '
                    '"Threats": "one sentence on key risks (competition, regulation, macro)", '
                    '"AI_Impact": "one sentence on how AI affects this company moat", '
                    '"Narrative_Score": <integer 0-100 where 0=very bearish, 50=neutral, 100=very bullish> }\n'
                    "Base the score on recent news, earnings trend, and analyst sentiment. "
                    "Return ONLY the JSON object, nothing else."
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
    Loads top 30 stocks split into 3 dedicated pools:

    COURT TERME pool (10 stocks) — calibrated for +15% to +40%:
        High momentum (Momentum_1Y > median), high Short_Interest (squeeze potential),
        Bullish_Divergence preferred, high Quant_Risk_Score.

    MOYEN TERME pool (10 stocks) — calibrated for +30% to +80%:
        Hurst > 0.55 (strong trend), Price > VWAP, RS_vs_SPY > 0 (outperforming market),
        Momentum_1Y > 20%, sorted by Quant_Risk_Score.

    LONG TERME pool (10 stocks) — calibrated for +30% to +150%:
        Margin_of_Safety > 0 (undervalued), Deep_Value_Score > 50,
        Fundamental_Score > 50, sorted by Deep_Value_Score.

    Perplexity is called on all 30 (deduplicated). Ultimate_Conviction_Score
    uses all 5 normalized scores.
    """
    df = pd.read_csv("sentiment.csv")
    if df.empty:
        print("Error: sentiment.csv is empty — run 04_sentiment_and_export.py first.")
        return pd.DataFrame()

    # Enrich with fundamentals and deep valuation for pre-filtering
    try:
        fund_df = pd.read_csv("fundamentals.csv")
        fund_add = [c for c in fund_df.columns if c not in df.columns and c != "ticker"]
        df = df.merge(fund_df[["ticker"] + fund_add], on="ticker", how="left")
    except FileNotFoundError:
        pass
    try:
        dv_df = pd.read_csv("deep_valuation.csv")
        dv_add = [c for c in dv_df.columns if c not in df.columns and c != "ticker"]
        df = df.merge(dv_df[["ticker"] + dv_add], on="ticker", how="left")
    except FileNotFoundError:
        pass

    # ══════════════════════════════════════════════════════════════════════════
    # STRICT TRI-STRATEGY SEGREGATION — No blending, no cannibalization
    # Each pool draws from its own source with its own risk/reward criteria.
    # Exactly top 5 per strategy → 15 tickers sent to Perplexity.
    # ══════════════════════════════════════════════════════════════════════════

    # ── COURT TERME pool (top 5): High Beta + High VaR + High Momentum ───────
    # Source: quant_risk.csv (full universe, has Beta + VaR_95)
    # Embrace volatility — CT wants explosive small/mid caps
    try:
        ct_source = pd.read_csv("quant_risk.csv")
        # Enrich with fundamentals for Short_Interest_Pct and Momentum_1Y
        try:
            fund_ct = pd.read_csv("fundamentals.csv")
            ct_add = [c for c in ["Short_Interest_Pct", "Momentum_1Y"]
                      if c in fund_ct.columns and c not in ct_source.columns]
            if ct_add:
                ct_source = ct_source.merge(fund_ct[["ticker"] + ct_add], on="ticker", how="left")
        except Exception:
            pass
        # Clean _x/_y
        for col in list(ct_source.columns):
            if col.endswith("_x"):
                base = col[:-2]
                y = base + "_y"
                if y in ct_source.columns:
                    ct_source[base] = ct_source[y].combine_first(ct_source[col])
                    ct_source.drop(columns=[col, y], inplace=True)
                else:
                    ct_source.rename(columns={col: base}, inplace=True)
            elif col.endswith("_y") and col[:-2] not in ct_source.columns:
                ct_source.rename(columns={col: col[:-2]}, inplace=True)
        ct_df = ct_source.copy()
    except FileNotFoundError:
        ct_df = df.copy()

    # CT_Score: Momentum*35 + Beta*25 + VaR*20 + ShortInterest*20
    mom  = ct_df["Momentum_1Y"].fillna(0).clip(0, 300) if "Momentum_1Y" in ct_df.columns else pd.Series(0, index=ct_df.index)
    beta = ct_df["Beta"].fillna(1).clip(0, 4)          if "Beta"         in ct_df.columns else pd.Series(1, index=ct_df.index)
    var  = ct_df["VaR_95"].fillna(0).abs()             if "VaR_95"       in ct_df.columns else pd.Series(0, index=ct_df.index)
    si   = ct_df["Short_Interest_Pct"].fillna(0).clip(0, 0.5) if "Short_Interest_Pct" in ct_df.columns else pd.Series(0, index=ct_df.index)
    ct_df["CT_Score"] = (
        (mom / mom.max().clip(1))   * 35
        + (beta / 4)                * 25
        + (var / var.max().clip(1)) * 20
        + (si / 0.5)                * 20
    )
    ct_pool = ct_df.nlargest(5, "CT_Score").copy()
    ct_pool["_pool"] = "court"
    print(f"  CT pool (top 5): {ct_pool['ticker'].tolist()}")

    # ── MOYEN TERME pool (top 5): Hurst>0.5 + Price>SMA_200 + RS>0 ──────────
    # Source: quant_risk.csv (full universe, has Hurst + SMA_200)
    try:
        mt_source = pd.read_csv("quant_risk.csv")
        # Clean _x/_y
        for col in list(mt_source.columns):
            if col.endswith("_x"):
                base = col[:-2]
                y = base + "_y"
                if y in mt_source.columns:
                    mt_source[base] = mt_source[y].combine_first(mt_source[col])
                    mt_source.drop(columns=[col, y], inplace=True)
                else:
                    mt_source.rename(columns={col: base}, inplace=True)
            elif col.endswith("_y") and col[:-2] not in mt_source.columns:
                mt_source.rename(columns={col: col[:-2]}, inplace=True)
        mt_df = mt_source.copy()
    except FileNotFoundError:
        mt_df = df.copy()

    # Strict MT filters — relax progressively if not enough candidates
    mt_filtered = pd.DataFrame()
    for hurst_min, require_sma200, require_rs in [
        (0.55, True,  True),
        (0.50, True,  False),
        (0.50, False, False),
        (0.0,  False, False),
    ]:
        mask = pd.Series(True, index=mt_df.index)
        if "Hurst_Exponent" in mt_df.columns and hurst_min > 0:
            mask &= mt_df["Hurst_Exponent"] > hurst_min
        if require_sma200 and "SMA_200" in mt_df.columns and "Last_Price" in mt_df.columns:
            mask &= mt_df["Last_Price"].fillna(0) > mt_df["SMA_200"].fillna(0)
        if require_rs and "RS_vs_SPY" in mt_df.columns:
            mask &= mt_df["RS_vs_SPY"] > 0
        # Exclude CT tickers
        mask &= ~mt_df["ticker"].isin(ct_pool["ticker"].tolist())
        mt_filtered = mt_df[mask]
        if len(mt_filtered) >= 5:
            break
    if mt_filtered.empty:
        mt_filtered = mt_df[~mt_df["ticker"].isin(ct_pool["ticker"].tolist())]
    mt_sort = [c for c in ["Hurst_Exponent", "RS_vs_SPY", "Quant_Risk_Score"] if c in mt_filtered.columns]
    mt_pool = mt_filtered.nlargest(5, mt_sort[0] if mt_sort else "Quant_Risk_Score").copy()
    mt_pool["_pool"] = "moyen"
    print(f"  MT pool (top 5): {mt_pool['ticker'].tolist()}")

    # ── LONG TERME pool (top 5): MoS>0 + Deep_Value_Score>50 ────────────────
    # Source: deep_valuation.csv (full universe, has Margin_of_Safety)
    try:
        lt_source = pd.read_csv("deep_valuation.csv")
        if "Fundamental_Score" not in lt_source.columns:
            try:
                fund_lt = pd.read_csv("fundamentals.csv")[["ticker", "Fundamental_Score"]]
                lt_source = lt_source.merge(fund_lt, on="ticker", how="left")
            except Exception:
                pass
        lt_df = lt_source.copy()
    except FileNotFoundError:
        lt_df = df.copy()

    # Strict LT filters — relax progressively
    lt_filtered = pd.DataFrame()
    ct_mt_tickers = ct_pool["ticker"].tolist() + mt_pool["ticker"].tolist()
    for mos_min, dv_min in [(0.10, 55), (0.0, 50), (0.0, 40), (None, 0)]:
        mask = ~lt_df["ticker"].isin(ct_mt_tickers)
        if mos_min is not None and "Margin_of_Safety" in lt_df.columns:
            mask &= lt_df["Margin_of_Safety"] > mos_min
        if dv_min > 0 and "Deep_Value_Score" in lt_df.columns:
            mask &= lt_df["Deep_Value_Score"] > dv_min
        lt_filtered = lt_df[mask]
        if len(lt_filtered) >= 5:
            break
    if lt_filtered.empty:
        lt_filtered = lt_df[~lt_df["ticker"].isin(ct_mt_tickers)]
    lt_sort = "Margin_of_Safety" if "Margin_of_Safety" in lt_filtered.columns else "Deep_Value_Score"
    lt_pool = lt_filtered.nlargest(5, lt_sort).copy()
    lt_pool["_pool"] = "long"
    print(f"  LT pool (top 5): {lt_pool['ticker'].tolist()}")

    # ── Combine exactly 15 tickers (5+5+5), deduplicate preserving pool tag ──
    combined = pd.concat([ct_pool, mt_pool, lt_pool], ignore_index=True)
    combined.drop_duplicates(subset="ticker", keep="first", inplace=True)
    combined.reset_index(drop=True, inplace=True)
    print(f"  Sending {len(combined)} unique tickers to Perplexity (CT=5, MT=5, LT=5)")

    top15 = combined

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

    # Normalize Finbert_Score from [-1,+1] to [0,100]
    if "Finbert_Score" in merged.columns:
        merged["Finbert_Score_N"] = ((merged["Finbert_Score"].fillna(0) + 1) / 2 * 100).clip(0, 100)
    else:
        merged["Finbert_Score_N"] = 50.0

    # Merge Fundamental_Score and Deep_Value_Score if available
    for extra_csv, score_col in [("fundamentals.csv", "Fundamental_Score"), ("deep_valuation.csv", "Deep_Value_Score")]:
        if score_col not in merged.columns:
            try:
                extra = pd.read_csv(extra_csv)[["ticker", score_col]]
                merged = merged.merge(extra, on="ticker", how="left")
            except Exception:
                merged[score_col] = 50.0
        merged[score_col] = pd.to_numeric(merged.get(score_col, 50), errors="coerce").fillna(50).clip(0, 100)

    # Ultimate_Conviction_Score: weighted sum of all 5 normalized scores (0-100)
    merged["Ultimate_Conviction_Score"] = (
        merged["Quant_Risk_Score"].clip(0, 100)  * _W_QUANT
        + merged["Narrative_Score"]              * _W_NARR
        + merged["Fundamental_Score"]            * _W_FUND
        + merged["Finbert_Score_N"]              * _W_FINBERT
        + merged["Deep_Value_Score"]             * _W_DEEPVAL
    ).round(2)

    merged.drop(columns=["Finbert_Score_N"], errors="ignore", inplace=True)

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
