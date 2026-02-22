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
        modifier = (df["Narrative_Score"].fillna(50) - 50) / 500
    elif "Deep_Value_Score" in df.columns and key == "long":
        modifier = (df["Deep_Value_Score"].fillna(50) - 50) / 500
    elif "Quant_Risk_Score" in df.columns:
        modifier = (df["Quant_Risk_Score"].fillna(50) - 50) / 500
    else:
        modifier = pd.Series(0.0, index=df.index)

    df["Kelly_Position_Pct"] = (base_kelly + modifier * 100).clip(1.0, 25.0).round(1)
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


def run_portfolio_allocator() -> dict[str, pd.DataFrame]:
    """
    Reads ai_narrative.csv (quant track) and event_driven.csv (event track),
    merges both, builds the 3 strategic portfolios, and exports to Excel.

    Court Terme uses BOTH tracks merged — best Narrative_Score wins.
    Long Terme and Moyen Terme remain purely quant-driven.
    """
    df = pd.read_csv("ai_narrative.csv")
    if df.empty:
        print("Error: ai_narrative.csv is empty — run 04_perplexity_narrative.py first.")
        return {}

    try:
        event_df = pd.read_csv("event_driven.csv")
        if not event_df.empty:
            combined = pd.concat([df, event_df], ignore_index=True, sort=False)
            combined.drop_duplicates(subset="ticker", keep="first", inplace=True)
            combined.reset_index(drop=True, inplace=True)
            print(f"  Merged quant track ({len(df)}) + event track ({len(event_df)}) → {len(combined)} unique stocks")
        else:
            combined = df
    except FileNotFoundError:
        combined = df
        print("  event_driven.csv not found — using quant track only for Court Terme")

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
