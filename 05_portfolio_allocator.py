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
    "Momentum_1M",
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
    Strict Tri-Strategy Bifurcation — zero overlap, distinct risk/reward per portfolio.

    COURT TERME (1-30 days | Catalyst):
        Source: _pool=="court" (event-driven + Perplexity CT candidates)
        CT_Score = Relative_Volume*30 + Momentum_1M*25 + Short_Interest*25 + ATR_14*20
        Sort: CT_Score → Narrative_Score

    MOYEN TERME (1-8 months | Momentum):
        Source: _pool=="moyen" (quant_risk full universe)
        Filters: Hurst > 0.52 + Price > SMA_200 + Top10_Institutional > 20%
        MT_Score = Hurst*35 + Institutional*30 + RS_vs_SPY*20 + QR*15

    LONG TERME (1+ years | Fortress Value):
        Source: _pool=="long" (deep_valuation full universe)
        Hard Gates: Piotroski_F_Score >= 7 AND Altman_Z_Score >= 2.99
        Sort: Margin_of_Safety → Deep_Value_Score → Fundamental_Score
    """
    available = [c for c in _OUTPUT_COLS if c in df.columns]

    # ── COURT TERME: Liquidity surge + Intraday vol + Squeeze ──────────────
    ct_cands = _pool_candidates(df, "court", exclude_tickers=[])
    ct_cands = ct_cands.copy()
    # Recompute CT_Score with institutional-grade metrics
    rvol  = ct_cands["Relative_Volume"].rank(pct=True, na_option="bottom")       if "Relative_Volume"    in ct_cands.columns else pd.Series(0.5, index=ct_cands.index)
    mom1m = ct_cands["Momentum_1M"].rank(pct=True, na_option="bottom")           if "Momentum_1M"        in ct_cands.columns else pd.Series(0.5, index=ct_cands.index)
    si    = ct_cands["Short_Interest_Pct"].rank(pct=True, na_option="bottom")    if "Short_Interest_Pct" in ct_cands.columns else pd.Series(0.5, index=ct_cands.index)
    atr   = ct_cands["ATR_14"].rank(pct=True, na_option="bottom")               if "ATR_14"             in ct_cands.columns else pd.Series(0.5, index=ct_cands.index)
    ct_cands["CT_Score"] = rvol * 30 + mom1m * 25 + si * 25 + atr * 20
    # Sort: CT_Score → Narrative_Score
    ct_sort = ["CT_Score"]
    if "Narrative_Score" in ct_cands.columns:
        ct_sort.append("Narrative_Score")
    short_term = (
        ct_cands.sort_values(ct_sort, ascending=[False] * len(ct_sort))
        .head(5)[available].reset_index(drop=True)
    )
    ct_tickers = short_term["ticker"].tolist()

    # ── MOYEN TERME: Hurst + Institutional + Price>SMA_200 ─────────────────
    mt_cands = _pool_candidates(df, "moyen", exclude_tickers=ct_tickers)
    # Progressive filter: Hurst + SMA_200 + Institutional — relax progressively
    filtered_mt = pd.DataFrame()
    for hurst_min, require_sma200, require_inst in [
        (0.52, True,  True),
        (0.50, True,  False),
        (0.48, False, False),
        (0.0,  False, False),
    ]:
        mask = pd.Series(True, index=mt_cands.index)
        if "Hurst_Exponent" in mt_cands.columns and hurst_min > 0:
            mask &= mt_cands["Hurst_Exponent"] > hurst_min
        if require_sma200 and "SMA_200" in mt_cands.columns:
            price_col = next((c for c in ["Last_Price", "Current_Price"] if c in mt_cands.columns), None)
            if price_col:
                mask &= mt_cands[price_col].fillna(0) > mt_cands["SMA_200"].fillna(0)
        if require_inst and "Top10_Institutional_Pct" in mt_cands.columns:
            mask &= mt_cands["Top10_Institutional_Pct"].fillna(0) > 0.20
        filtered_mt = mt_cands[mask]
        if len(filtered_mt) >= 5:
            break
    if filtered_mt.empty:
        filtered_mt = mt_cands
    # MT_Score: Hurst*35 + Institutional*30 + RS_vs_SPY*20 + QR*15
    filtered_mt = filtered_mt.copy()
    h_r  = filtered_mt["Hurst_Exponent"].rank(pct=True, na_option="bottom")       if "Hurst_Exponent"       in filtered_mt.columns else pd.Series(0.5, index=filtered_mt.index)
    i_r  = filtered_mt["Top10_Institutional_Pct"].rank(pct=True, na_option="bottom") if "Top10_Institutional_Pct" in filtered_mt.columns else pd.Series(0.5, index=filtered_mt.index)
    rs_r = filtered_mt["RS_vs_SPY"].rank(pct=True, na_option="bottom")            if "RS_vs_SPY"            in filtered_mt.columns else pd.Series(0.5, index=filtered_mt.index)
    qr_r = filtered_mt["Quant_Risk_Score"].rank(pct=True, na_option="bottom")     if "Quant_Risk_Score"     in filtered_mt.columns else pd.Series(0.5, index=filtered_mt.index)
    filtered_mt["MT_Score"] = h_r * 35 + i_r * 30 + rs_r * 20 + qr_r * 15
    medium_term = (
        filtered_mt.sort_values("MT_Score", ascending=False)
        .head(5)[available].reset_index(drop=True)
    )
    mt_tickers = medium_term["ticker"].tolist()

    # ── LONG TERME: Fortress Value — MoS + Piotroski + Altman_Z ────────────
    lt_cands = _pool_candidates(df, "long", exclude_tickers=ct_tickers + mt_tickers)
    # Progressive filter: Piotroski + Altman_Z hard gates → relax progressively
    filtered_lt = pd.DataFrame()
    for mos_min, dv_min, pio_min, alt_min in [
        (0.10, 55, 7, 2.99),   # Strict: strong balance sheet + safe zone
        (0.10, 40, 6, 2.50),   # Relax quality slightly
        (0.05, 30, 5, 1.81),   # Grey zone Altman but decent Piotroski
        (0.0,  0,  0, 0),      # Last resort: any undervalued stock
    ]:
        mask = pd.Series(True, index=lt_cands.index)
        if mos_min is not None and "Margin_of_Safety" in lt_cands.columns:
            mask &= lt_cands["Margin_of_Safety"] > mos_min
        if dv_min > 0 and "Deep_Value_Score" in lt_cands.columns:
            mask &= lt_cands["Deep_Value_Score"] > dv_min
        if pio_min > 0 and "Piotroski_F_Score" in lt_cands.columns:
            mask &= lt_cands["Piotroski_F_Score"].fillna(0) >= pio_min
        if alt_min > 0 and "Altman_Z_Score" in lt_cands.columns:
            mask &= lt_cands["Altman_Z_Score"].fillna(0) >= alt_min
        filtered_lt = lt_cands[mask]
        if len(filtered_lt) >= 5:
            break
    if filtered_lt.empty:
        filtered_lt = lt_cands
    lt_sort = [c for c in ["Margin_of_Safety", "Deep_Value_Score", "Fundamental_Score"] if c in filtered_lt.columns]
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
    Builds 3 portfolios from distinct data sources, fully enriched:

    COURT TERME  → ai_narrative.csv + event_driven.csv  (Perplexity + event)
    MOYEN TERME  → quant_risk.csv   (full universe, momentum/trend)
    LONG TERME   → deep_valuation.csv (full universe, deep value)

    Each pool is enriched with ALL available data sources before selection,
    so every stock has Last_Price, Margin_of_Safety, VaR_95, etc.
    """
    # ── Load primary sources ─────────────────────────────────────────────────
    df = pd.read_csv("ai_narrative.csv")
    if df.empty:
        print("Error: ai_narrative.csv is empty — run 04_perplexity_narrative.py first.")
        return {}

    try:
        quant_df = pd.read_csv("quant_risk.csv")
    except FileNotFoundError:
        quant_df = df.copy()
        print("  quant_risk.csv not found — using ai_narrative for MT pool")

    try:
        dv_full = pd.read_csv("deep_valuation.csv")
    except FileNotFoundError:
        dv_full = df.copy()
        print("  deep_valuation.csv not found — using ai_narrative for LT pool")

    # ── Load enrichment sources once ─────────────────────────────────────────
    try:
        fund_src = pd.read_csv("fundamentals.csv")
    except FileNotFoundError:
        fund_src = pd.DataFrame()
        print("  fundamentals.csv not found — skipping fundamental enrichment")

    try:
        dv_src = pd.read_csv("deep_valuation.csv")
    except FileNotFoundError:
        dv_src = pd.DataFrame()

    try:
        qr_src = pd.read_csv("quant_risk.csv")
    except FileNotFoundError:
        qr_src = pd.DataFrame()

    def _enrich(pool: pd.DataFrame, *sources: pd.DataFrame) -> pd.DataFrame:
        """Add missing columns AND fill NaN in existing columns from sources."""
        for src in sources:
            if src.empty or "ticker" not in src.columns:
                continue
            src_dedup = src.drop_duplicates(subset="ticker", keep="first")
            # 1) Add brand-new columns via merge
            new_cols = [c for c in src_dedup.columns if c not in pool.columns and c != "ticker"]
            if new_cols:
                pool = pool.merge(src_dedup[["ticker"] + new_cols], on="ticker", how="left")
            # 2) Fill NaN in existing columns via map lookup
            fill_cols = [c for c in src_dedup.columns
                         if c in pool.columns and c != "ticker" and pool[c].isna().any()]
            if fill_cols:
                lookup = src_dedup.set_index("ticker")
                for col in fill_cols:
                    if col in lookup.columns:
                        filler = pool["ticker"].map(lookup[col])
                        pool[col] = pool[col].combine_first(filler)
        return _clean_columns(pool)

    _PLACEHOLDER = "Analyse en cours — relancer le pipeline pour les narratives."

    def _fill_placeholders(pool: pd.DataFrame) -> pd.DataFrame:
        for col in ["Catalysts", "Threats", "AI_Impact"]:
            if col in pool.columns:
                pool[col] = pool[col].fillna(_PLACEHOLDER)
        return pool

    # ── Merge event-driven into CT pool BEFORE enrichment ────────────────────
    try:
        event_df = pd.read_csv("event_driven.csv")
        if not event_df.empty:
            event_df["_pool"] = "court"
            df = pd.concat([df, event_df], ignore_index=True, sort=False)
            df.drop_duplicates(subset="ticker", keep="first", inplace=True)
            df = _clean_columns(df)
            print(f"  Event track merged into CT pool → {len(df)} CT candidates")
    except FileNotFoundError:
        print("  event_driven.csv not found — CT pool uses Perplexity track only")

    if "_pool" not in df.columns:
        df["_pool"] = "court"

    # ── Enrich CT pool with ALL sources (fund + dv + quant_risk) ─────────────
    df = _enrich(df, fund_src, dv_src, qr_src)
    df = _fill_placeholders(df)

    # ── Build MT pool from quant_risk.csv enriched ──────────────────────────
    quant_df["_pool"] = "moyen"
    quant_df = _enrich(quant_df, fund_src, dv_src)
    # Carry over Perplexity narrative data for any overlapping tickers
    narr_cols = [c for c in ["Catalysts", "Threats", "AI_Impact", "Narrative_Score",
                              "Ultimate_Conviction_Score", "Finbert_Score"] if c in df.columns]
    if narr_cols:
        quant_df = quant_df.merge(df[["ticker"] + narr_cols], on="ticker", how="left")
        quant_df = _clean_columns(quant_df)
    quant_df = _fill_placeholders(quant_df)

    # ── Build LT pool from deep_valuation.csv enriched with quant_risk ──────
    dv_full_e = _enrich(dv_full, fund_src, qr_src)
    dv_full_e["_pool"] = "long"
    if narr_cols:
        dv_full_e = dv_full_e.merge(df[["ticker"] + narr_cols], on="ticker", how="left")
        dv_full_e = _clean_columns(dv_full_e)
    dv_full_e = _fill_placeholders(dv_full_e)

    # ── Combine all 3 pools into one df with _pool tags ─────────────────────
    combined = pd.concat([df, quant_df, dv_full_e], ignore_index=True, sort=False)
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
