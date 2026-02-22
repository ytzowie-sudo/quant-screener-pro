import numpy as np
import pandas as pd


def _kelly_criterion(win_rate: float, avg_win: float, avg_loss: float) -> float:
    """
    Kelly Criterion — optimal fraction of capital to allocate.
    f* = (p * b - q) / b
        p = win probability
        b = avg win / avg loss ratio
        q = 1 - p (loss probability)

    Uses conservative half-Kelly to reduce variance.
    Returns a % between 0 and 25% (capped for safety).
    """
    try:
        if avg_loss == 0 or win_rate <= 0 or win_rate >= 1:
            return 5.0
        b = avg_win / avg_loss
        q = 1 - win_rate
        kelly = (win_rate * b - q) / b
        half_kelly = kelly / 2
        return round(max(0.0, min(half_kelly * 100, 25.0)), 1)
    except Exception:
        return 5.0


def _add_kelly(df: pd.DataFrame, portfolio_type: str) -> pd.DataFrame:
    """
    Adds a Kelly_Position_Pct column based on portfolio type assumptions:
    - Court Terme : win_rate=0.55, avg_win=0.25, avg_loss=0.08
    - Moyen Terme : win_rate=0.60, avg_win=0.50, avg_loss=0.15
    - Long Terme  : win_rate=0.65, avg_win=1.00, avg_loss=0.20
    Adjusted per stock by Narrative_Score or Deep_Value_Score if available.
    """
    params = {
        "court": (0.55, 0.25, 0.08),
        "moyen": (0.60, 0.50, 0.15),
        "long":  (0.65, 1.00, 0.20),
    }
    key = "court" if "Court" in portfolio_type else ("moyen" if "Moyen" in portfolio_type else "long")
    wr, aw, al = params[key]
    base_kelly = _kelly_criterion(wr, aw, al)

    if "Narrative_Score" in df.columns and key == "court":
        modifier = (df["Narrative_Score"].fillna(50) - 50) / 200
    elif "Deep_Value_Score" in df.columns and key == "long":
        modifier = (df["Deep_Value_Score"].fillna(50) - 50) / 200
    elif "Quant_Risk_Score" in df.columns:
        modifier = (df["Quant_Risk_Score"].fillna(50) - 50) / 200
    else:
        modifier = pd.Series(0.0, index=df.index)

    df["Kelly_Position_Pct"] = (base_kelly + modifier * 50).clip(1.0, 25.0).round(1)
    return df

_OUTPUT_COLS = [
    "ticker",
    "Sector",
    "Industry",
    "Ultimate_Conviction_Score",
    "Fundamental_Score",
    "Deep_Value_Score",
    "Quant_Risk_Score",
    "Margin_of_Safety",
    "Last_Price",
    "Analyst_Target",
    "Analyst_Rec",
    "52W_High",
    "52W_Low",
    "Price_vs_52W_High",
    "VaR_95",
    "Price_vs_VWAP",
    "Hurst_Exponent",
    "RS_vs_SPY",
    "Momentum_1Y",
    "Bullish_Divergence",
    "SMA_200",
    "Stoch_K",
    "Stoch_D",
    "Short_Interest_Pct",
    "Short_Ratio",
    "Insider_Buy_Pct",
    "Next_Earnings_Date",
    "Dividend_Yield",
    "Book_Value",
    "Price_to_Book",
    "Top10_Institutional_Pct",
    "Narrative_Score",
    "Finbert_Score",
    "Catalysts",
    "Threats",
    "AI_Impact",
    "Kelly_Position_Pct",
    "Piotroski_F_Score",
    "Altman_Z_Score",
    "Beta",
]

_OUTPUT_FILE = "Hedge_Fund_Master_Strategy.xlsx"


def _pool_candidates(df: pd.DataFrame, pool_tag: str, exclude_tickers: list = None) -> pd.DataFrame:
    """
    Returns candidates for a pool filtered by _pool tag.
    Excludes tickers already assigned to higher-priority portfolios.
    """
    pool_df = df[df["_pool"] == pool_tag].copy() if "_pool" in df.columns else df.copy()
    if exclude_tickers:
        pool_df = pool_df[~pool_df["ticker"].isin(exclude_tickers)]
    return pool_df if not pool_df.empty else df[~df["ticker"].isin(exclude_tickers or [])].copy()


def build_portfolios(df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """
    Builds 3 portfolios from pre-filtered pools (set by 04_perplexity_narrative.py).

    COURT TERME (+15% to +40%):
        Pool: high momentum + short squeeze candidates
        Sort: Bullish_Divergence → Ultimate_Conviction_Score → Quant_Risk_Score
        Extra filter: Momentum_1Y > 15% preferred

    MOYEN TERME (+30% to +80%):
        Pool: Hurst > 0.55 + RS_vs_SPY > 0 (outperforming market)
        Sort: Quant_Risk_Score → RS_vs_SPY
        Extra filter: Price > VWAP + Price > SMA_200

    LONG TERME (+30% to +150%):
        Pool: Margin_of_Safety > 0 + Deep_Value_Score > 50
        Sort: Deep_Value_Score → Margin_of_Safety → Fundamental_Score
    """
    available = [c for c in _OUTPUT_COLS if c in df.columns]

    # ── COURT TERME: momentum + catalyst candidates ────────────────────────────
    ct_cands = _pool_candidates(df, "court", exclude_tickers=[])
    if "Momentum_1Y" in ct_cands.columns:
        strong = ct_cands[ct_cands["Momentum_1Y"] > 15]
        if len(strong) >= 5:
            ct_cands = strong
    if "Bullish_Divergence" in ct_cands.columns:
        ct_cands = ct_cands.copy()
        ct_cands["_bd_int"] = ct_cands["Bullish_Divergence"].fillna(0).astype(int)
    narr_std = ct_cands["Narrative_Score"].std() if "Narrative_Score" in ct_cands.columns else 0
    ct_sort = []
    if "_bd_int" in ct_cands.columns:
        ct_sort.append("_bd_int")
    if "CT_Score" in ct_cands.columns:
        ct_sort.append("CT_Score")
    elif narr_std > 2 and "Narrative_Score" in ct_cands.columns:
        ct_sort.append("Narrative_Score")
    elif "Ultimate_Conviction_Score" in ct_cands.columns:
        ct_sort.append("Ultimate_Conviction_Score")
    if "Quant_Risk_Score" in ct_cands.columns:
        ct_sort.append("Quant_Risk_Score")
    short_term = (
        ct_cands.sort_values(ct_sort, ascending=[False] * len(ct_sort))
        .head(5)[available].reset_index(drop=True)
    )
    ct_tickers = short_term["ticker"].tolist()

    # ── MOYEN TERME: trending + outperforming market ───────────────────────────
    mt_cands = _pool_candidates(df, "moyen", exclude_tickers=ct_tickers)
    # Apply filters progressively — relax if not enough candidates
    for hurst_thresh, require_rs, require_vwap in [
        (0.55, True,  True),   # strict
        (0.50, True,  False),  # relax VWAP
        (0.50, False, False),  # relax RS too
        (0.00, False, False),  # no filter — fallback
    ]:
        mt_mask = pd.Series([True] * len(mt_cands), index=mt_cands.index)
        if "Hurst_Exponent" in mt_cands.columns and hurst_thresh > 0:
            mt_mask &= mt_cands["Hurst_Exponent"] > hurst_thresh
        if require_rs and "RS_vs_SPY" in mt_cands.columns:
            mt_mask &= mt_cands["RS_vs_SPY"] > 0
        if require_vwap and "Price_vs_VWAP" in mt_cands.columns:
            mt_mask &= mt_cands["Price_vs_VWAP"] > 0
        if "SMA_200" in mt_cands.columns:
            price_col = "Last_Price" if "Last_Price" in mt_cands.columns else ("Current_Price" if "Current_Price" in mt_cands.columns else None)
            if price_col:
                mt_mask &= mt_cands[price_col].fillna(0) > mt_cands["SMA_200"].fillna(0)
        filtered_mt = mt_cands[mt_mask]
        if len(filtered_mt) >= 5:
            break
    if filtered_mt.empty:
        filtered_mt = mt_cands
    mt_sort = [c for c in ["Quant_Risk_Score", "RS_vs_SPY", "Hurst_Exponent"] if c in filtered_mt.columns]
    medium_term = (
        filtered_mt.sort_values(mt_sort, ascending=[False] * len(mt_sort))
        .head(5)[available].reset_index(drop=True)
    )
    mt_tickers = medium_term["ticker"].tolist()

    # ── LONG TERME: undervalued + strong fundamentals ─────────────────────────
    lt_cands = _pool_candidates(df, "long", exclude_tickers=ct_tickers + mt_tickers)
    # Progressive filter: strict → relax Fundamental_Score → relax MoS
    for mos_min, dv_min, fund_min in [
        (0.10, 50, 50),   # strict: MoS>10%, DV>50, Fund>50
        (0.0,  50, 40),   # relax Fund threshold
        (0.0,  40, 0),    # relax DV threshold
        (None, 0,  0),    # no filter
    ]:
        lt_mask = pd.Series([True] * len(lt_cands), index=lt_cands.index)
        if mos_min is not None and "Margin_of_Safety" in lt_cands.columns:
            lt_mask &= lt_cands["Margin_of_Safety"] > mos_min
        if dv_min > 0 and "Deep_Value_Score" in lt_cands.columns:
            lt_mask &= lt_cands["Deep_Value_Score"] > dv_min
        if fund_min > 0 and "Fundamental_Score" in lt_cands.columns:
            lt_mask &= lt_cands["Fundamental_Score"] > fund_min
        filtered_lt = lt_cands[lt_mask]
        if len(filtered_lt) >= 5:
            break
    if filtered_lt.empty:
        filtered_lt = lt_cands
    lt_sort = [c for c in ["Deep_Value_Score", "Margin_of_Safety", "Fundamental_Score", "Ultimate_Conviction_Score"] if c in filtered_lt.columns]
    long_term = (
        filtered_lt.sort_values(lt_sort, ascending=[False] * len(lt_sort))
        .head(5)[available].reset_index(drop=True)
    )

    short_term  = _add_kelly(short_term,  "Court Terme")
    medium_term = _add_kelly(medium_term, "Moyen Terme")
    long_term   = _add_kelly(long_term,   "Long Terme")

    return {
        "Court Terme (Catalysts)": short_term,
        "Moyen Terme (Momentum)":  medium_term,
        "Long Terme (Value)":      long_term,
    }


def export_to_excel(portfolios: dict[str, pd.DataFrame], path: str = _OUTPUT_FILE) -> None:
    """Writes each portfolio to a named sheet in a single Excel workbook."""
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        for sheet_name, portfolio_df in portfolios.items():
            portfolio_df.to_excel(writer, sheet_name=sheet_name, index=False)


def _clean_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Resolves duplicate column suffixes (_x / _y) created by successive merges.
    Keeps _y (the richer source) when both exist, then renames to base name.
    Also ensures Last_Price is always present.
    """
    for col in list(df.columns):
        if col.endswith("_x"):
            base = col[:-2]
            y_col = base + "_y"
            if y_col in df.columns:
                df[base] = df[y_col].combine_first(df[col])
                df.drop(columns=[col, y_col], inplace=True)
            else:
                df.rename(columns={col: base}, inplace=True)
        elif col.endswith("_y") and col[:-2] not in df.columns:
            df.rename(columns={col: col[:-2]}, inplace=True)

    if "Last_Price" not in df.columns:
        for fallback in ["VWAP", "Last_Price_y", "Last_Price_x"]:
            if fallback in df.columns:
                df["Last_Price"] = df[fallback]
                break

    return df


def run_portfolio_allocator() -> dict[str, pd.DataFrame]:
    """
    Builds 3 portfolios from distinct data sources:

    COURT TERME  → ai_narrative.csv (top 30 Perplexity stocks, narrative quality)
    MOYEN TERME  → quant_risk.csv   (top 100 quant stocks, momentum/trend quality)
    LONG TERME   → deep_valuation.csv (all 590 stocks, value quality)

    Each pool is enriched with fundamentals + deep_valuation before selection.
    """
    # ── Load Perplexity track (Court Terme base) ───────────────────────────────
    df = pd.read_csv("ai_narrative.csv")
    if df.empty:
        print("Error: ai_narrative.csv is empty — run 04_perplexity_narrative.py first.")
        return {}

    # ── Load wider quant pool for MT (100 stocks) ─────────────────────────────
    try:
        quant_df = pd.read_csv("quant_risk.csv")
    except FileNotFoundError:
        quant_df = df.copy()
        print("  quant_risk.csv not found — using ai_narrative for MT pool")

    # ── Load deep value pool for LT (all 590 stocks) ──────────────────────────
    try:
        dv_full = pd.read_csv("deep_valuation.csv")
    except FileNotFoundError:
        dv_full = df.copy()
        print("  deep_valuation.csv not found — using ai_narrative for LT pool")

    # ── Enrich with fundamentals (Last_Price, Margin_of_Safety, scores, etc.) ─
    try:
        fund_df = pd.read_csv("fundamentals.csv")
        fund_cols = [c for c in fund_df.columns if c not in df.columns or c == "ticker"]
        df = df.merge(fund_df[["ticker"] + [c for c in fund_cols if c != "ticker"]],
                      on="ticker", how="left")
    except FileNotFoundError:
        print("  fundamentals.csv not found — skipping fundamental enrichment")

    try:
        dv_df = pd.read_csv("deep_valuation.csv")
        dv_cols = [c for c in dv_df.columns if c not in df.columns or c == "ticker"]
        df = df.merge(dv_df[["ticker"] + [c for c in dv_cols if c != "ticker"]],
                      on="ticker", how="left")
    except FileNotFoundError:
        print("  deep_valuation.csv not found — skipping deep valuation enrichment")

    df = _clean_columns(df)

    # ── Fill NaN Catalysts/Threats/AI_Impact with placeholder ─────────────────
    for col in ["Catalysts", "Threats", "AI_Impact"]:
        if col in df.columns:
            df[col] = df[col].fillna("Analyse en cours — relancer le pipeline pour les narratives.")

    # ── Tag CT pool on Perplexity df ───────────────────────────────────────────
    if "_pool" not in df.columns:
        df["_pool"] = "court"

    # ── Build MT pool from quant_risk.csv (100 stocks) enriched ───────────────
    try:
        fund_df = pd.read_csv("fundamentals.csv")
        qf_add = [c for c in fund_df.columns if c not in quant_df.columns and c != "ticker"]
        quant_df = quant_df.merge(fund_df[["ticker"] + qf_add], on="ticker", how="left")
    except Exception:
        pass
    try:
        dv_df2 = pd.read_csv("deep_valuation.csv")
        qd_add = [c for c in dv_df2.columns if c not in quant_df.columns and c != "ticker"]
        quant_df = quant_df.merge(dv_df2[["ticker"] + qd_add], on="ticker", how="left")
    except Exception:
        pass
    quant_df = _clean_columns(quant_df)
    quant_df["_pool"] = "moyen"
    # Carry over Perplexity narrative data for any overlapping tickers
    narr_cols = [c for c in ["Catalysts", "Threats", "AI_Impact", "Narrative_Score",
                              "Ultimate_Conviction_Score", "Finbert_Score"] if c in df.columns]
    if narr_cols:
        quant_df = quant_df.merge(df[["ticker"] + narr_cols], on="ticker", how="left")
        quant_df = _clean_columns(quant_df)
    for col in ["Catalysts", "Threats", "AI_Impact"]:
        if col in quant_df.columns:
            quant_df[col] = quant_df[col].fillna("Analyse en cours — relancer le pipeline pour les narratives.")

    # ── Build LT pool from deep_valuation.csv (590 stocks) enriched ───────────
    try:
        fund_df2 = pd.read_csv("fundamentals.csv")
        lf_add = [c for c in fund_df2.columns if c not in dv_full.columns and c != "ticker"]
        dv_full_e = dv_full.merge(fund_df2[["ticker"] + lf_add], on="ticker", how="left")
    except Exception:
        dv_full_e = dv_full.copy()
    dv_full_e = _clean_columns(dv_full_e)
    dv_full_e["_pool"] = "long"
    if narr_cols:
        dv_full_e = dv_full_e.merge(df[["ticker"] + narr_cols], on="ticker", how="left")
        dv_full_e = _clean_columns(dv_full_e)
    for col in ["Catalysts", "Threats", "AI_Impact"]:
        if col in dv_full_e.columns:
            dv_full_e[col] = dv_full_e[col].fillna("Analyse en cours — relancer le pipeline pour les narratives.")

    # ── Merge event-driven track into CT pool ─────────────────────────────────
    try:
        event_df = pd.read_csv("event_driven.csv")
        if not event_df.empty:
            event_df["_pool"] = "court"
            df = pd.concat([df, event_df], ignore_index=True, sort=False)
            df.drop_duplicates(subset="ticker", keep="first", inplace=True)
            df = _clean_columns(df)
            print(f"  Event track merged into CT pool → {len(df)} CT candidates")
    except FileNotFoundError:
        print("  event_driven.csv not found — CT pool uses quant track only")

    # ── Combine all 3 pools into one df with _pool tags ───────────────────────
    combined = pd.concat([df, quant_df, dv_full_e], ignore_index=True, sort=False)
    # Keep first occurrence per ticker per pool (CT > MT > LT priority for dedup within pool)
    combined.drop_duplicates(subset=["ticker", "_pool"], keep="first", inplace=True)
    combined.reset_index(drop=True, inplace=True)
    combined = _clean_columns(combined)
    print(f"  Combined pools: CT={len(df)} MT={len(quant_df)} LT={len(dv_full_e)} stocks")

    portfolios = build_portfolios(combined)
    export_to_excel(portfolios)
    return portfolios


if __name__ == "__main__":
    portfolios = run_portfolio_allocator()

    labels = {
        "Court Terme (Catalysts)": "SHORT-TERM CATALYST PLAYS",
        "Moyen Terme (Momentum)":  "MEDIUM-TERM MOMENTUM PLAYS",
        "Long Terme (Value)":      "LONG-TERM FORTRESS (VALUE)",
    }

    print(f"\nPortfolio allocation complete → {_OUTPUT_FILE}\n")
    print("=" * 50)
    for sheet, title in labels.items():
        tickers = portfolios[sheet]["ticker"].tolist()
        print(f"\n  {title}")
        for i, t in enumerate(tickers, 1):
            print(f"    {i}. {t}")
    print("\n" + "=" * 50)
