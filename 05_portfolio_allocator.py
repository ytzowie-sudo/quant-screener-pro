import pandas as pd

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
]

_OUTPUT_FILE = "Hedge_Fund_Master_Strategy.xlsx"


def build_portfolios(df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """
    Splits the universe into 3 strategic portfolios with strict quant criteria:

    Long Term (Value):
        Requires high Margin_of_Safety (> 0) AND high Deep_Value_Score.
        Sorted by Deep_Value_Score desc, then Margin_of_Safety desc.

    Medium Term (Momentum):
        Requires Hurst_Exponent > 0.5 (trending) AND Price_vs_VWAP > 0
        (price above VWAP) AND Last_Price > SMA_200 (long-term uptrend).
        Sorted by Quant_Risk_Score desc.

    Short Term (Catalyst / Event-Driven):
        Requires strong AI Catalysts (Narrative_Score > 60) AND
        Bullish_Divergence = True preferred, else top Narrative_Score.
        Sorted by Narrative_Score desc, Bullish_Divergence desc.
    """
    available = [c for c in _OUTPUT_COLS if c in df.columns]

    # ── Long Term: genuine margin of safety + deep value ──────────────────────
    lt_mask = pd.Series([True] * len(df), index=df.index)
    if "Margin_of_Safety" in df.columns:
        lt_mask &= df["Margin_of_Safety"] > 0
    long_candidates = df[lt_mask].copy()
    if long_candidates.empty:
        long_candidates = df.copy()
    lt_sort_cols = [c for c in ["Deep_Value_Score", "Margin_of_Safety", "Ultimate_Conviction_Score", "Quant_Risk_Score"] if c in long_candidates.columns]
    long_term = (
        long_candidates
        .sort_values(lt_sort_cols, ascending=[False] * len(lt_sort_cols))
        .head(5)[available]
        .reset_index(drop=True)
    )

    # ── Medium Term: Hurst > 0.5, Price > VWAP, Price > SMA_200 ──────────────
    mt_mask = pd.Series([True] * len(df), index=df.index)
    if "Hurst_Exponent" in df.columns:
        mt_mask &= df["Hurst_Exponent"] > 0.5
    if "Price_vs_VWAP" in df.columns:
        mt_mask &= df["Price_vs_VWAP"] > 0
    if "Last_Price" in df.columns and "SMA_200" in df.columns:
        mt_mask &= df["Last_Price"] > df["SMA_200"]
    medium_candidates = df[mt_mask].copy()
    if medium_candidates.empty:
        medium_candidates = df.copy()
    medium_term = (
        medium_candidates
        .sort_values("Quant_Risk_Score", ascending=False)
        .head(5)[available]
        .reset_index(drop=True)
    )

    # ── Short Term: strong narrative + bullish divergence ─────────────────────
    st_mask = pd.Series([True] * len(df), index=df.index)
    if "Narrative_Score" in df.columns:
        st_mask &= df["Narrative_Score"] > 60
    short_candidates = df[st_mask].copy()
    if short_candidates.empty:
        short_candidates = df.copy()
    sort_cols = ["Narrative_Score"]
    sort_asc  = [False]
    if "Bullish_Divergence" in short_candidates.columns:
        short_candidates["_bd_int"] = short_candidates["Bullish_Divergence"].astype(int)
        sort_cols = ["_bd_int", "Narrative_Score"]
        sort_asc  = [False, False]
    short_term = (
        short_candidates
        .sort_values(sort_cols, ascending=sort_asc)
        .head(5)[available]
        .reset_index(drop=True)
    )

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


def run_portfolio_allocator() -> dict[str, pd.DataFrame]:
    """
    Reads narrative_analyzed.csv, builds the 3 strategic portfolios,
    exports to Hedge_Fund_Master_Strategy.xlsx, and returns the portfolios dict.
    """
    df = pd.read_csv("ai_narrative.csv")
    if df.empty:
        print("Error: ai_narrative.csv is empty — run 04_perplexity_narrative.py first.")
        return {}
    portfolios = build_portfolios(df)
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
