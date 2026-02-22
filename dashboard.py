import os
import subprocess
import sys
import time

import pandas as pd
import streamlit as st
import yfinance as yf

_EXCEL_FILE = "Hedge_Fund_Master_Strategy.xlsx"
_SHEET_MAP  = {
    "⚡ SHORT TERM (Event-Driven)": "Court Terme (Catalysts)",
    "📈 MEDIUM TERM (Momentum)":   "Moyen Terme (Momentum)",
    "🏰 LONG TERM (Deep Value)":   "Long Terme (Value)",
}

st.set_page_config(
    page_title="Quant Screener Pro",
    page_icon="🦅",
    layout="wide",
    initial_sidebar_state="collapsed",
)

st.markdown("""
<style>
    /* ── Global dark background ── */
    html, body, [data-testid="stAppViewContainer"],
    [data-testid="stMain"], [data-testid="block-container"] {
        background-color: #0A0A0F !important;
        color: #E8EAF0 !important;
    }

    /* ── Hide Streamlit chrome ── */
    #MainMenu, footer, header { visibility: hidden; }
    [data-testid="stToolbar"] { display: none; }

    /* ── Tabs ── */
    [data-testid="stTabs"] button {
        background: transparent !important;
        color: #7B8AB8 !important;
        font-size: 0.85rem;
        font-weight: 600;
        letter-spacing: 0.08em;
        border-bottom: 2px solid transparent !important;
        padding: 0.5rem 1.2rem !important;
    }
    [data-testid="stTabs"] button[aria-selected="true"] {
        color: #1E88E5 !important;
        border-bottom: 2px solid #1E88E5 !important;
    }

    /* ── Stock card container ── */
    .stock-card {
        background: #0F1117;
        border: 1px solid #1A2340;
        border-left: 3px solid #1E88E5;
        border-radius: 4px;
        padding: 1.2rem 1.4rem 1rem 1.4rem;
        margin-bottom: 0.6rem;
    }

    /* ── Ticker headline ── */
    .ticker-name {
        font-size: 1.5rem;
        font-weight: 800;
        color: #FFFFFF;
        letter-spacing: 0.06em;
    }
    .conviction-badge {
        display: inline-block;
        background: #1E88E5;
        color: #FFFFFF;
        font-size: 0.78rem;
        font-weight: 700;
        padding: 2px 10px;
        border-radius: 2px;
        margin-left: 10px;
        vertical-align: middle;
        letter-spacing: 0.05em;
    }

    /* ── Metric boxes ── */
    .metric-box {
        background: #13151F;
        border: 1px solid #1A2340;
        border-radius: 3px;
        padding: 0.6rem 0.9rem;
        text-align: center;
    }
    .metric-label {
        font-size: 0.65rem;
        color: #5C6A8A;
        letter-spacing: 0.1em;
        text-transform: uppercase;
        margin-bottom: 4px;
    }
    .metric-value {
        font-size: 1.05rem;
        font-weight: 700;
        color: #E8EAF0;
    }
    .metric-value.positive { color: #00C853; }
    .metric-value.negative { color: #FF1744; }
    .metric-value.blue     { color: #1E88E5; }

    /* ── Narrative boxes ── */
    .narrative-buy {
        background: #0D1F12;
        border: 1px solid #1B5E20;
        border-radius: 3px;
        padding: 0.6rem 1rem;
        margin-top: 0.5rem;
        font-size: 0.82rem;
        color: #A5D6A7;
        line-height: 1.5;
    }
    .narrative-risk {
        background: #1A0E0E;
        border: 1px solid #4A1010;
        border-radius: 3px;
        padding: 0.6rem 1rem;
        margin-top: 0.4rem;
        font-size: 0.82rem;
        color: #EF9A9A;
        line-height: 1.5;
    }

    /* ── Divider ── */
    hr { border-color: #1A2340 !important; margin: 1.2rem 0 !important; }

    /* ── Error box ── */
    .error-box {
        background: #1A0E0E;
        border: 1px solid #B71C1C;
        border-radius: 4px;
        padding: 2rem;
        text-align: center;
        color: #EF9A9A;
        font-size: 1rem;
    }
</style>
""", unsafe_allow_html=True)


# ── Header ────────────────────────────────────────────────────────────────────

st.markdown("""
<div style="padding: 1.2rem 0 0.4rem 0;">
    <span style="font-size:2.2rem; font-weight:900; color:#FFFFFF;
                 letter-spacing:0.06em;">🦅 QUANT SCREENER PRO</span>
    <span style="font-size:1rem; color:#1E88E5; font-weight:600;
                 margin-left:14px; letter-spacing:0.12em;">STRATEGY DESK  ·  V3.0</span>
</div>
<div style="font-size:0.75rem; color:#3A4A6A; letter-spacing:0.15em;
            margin-bottom:0.8rem;">
    INSTITUTIONAL-GRADE QUANTITATIVE HEDGE FUND ENGINE
</div>
""", unsafe_allow_html=True)

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

col1, col2, col3 = st.columns([1, 2, 1])
with col2:
    if st.button("🔄 LANCER LA MISE À JOUR DU MARCHÉ (5-10 min)", type="primary", use_container_width=True):
        with st.spinner("Exécution des 9 moteurs quantitatifs... (5-10 min)"):
            try:
                result = subprocess.run(
                    [sys.executable, "run_fund.py"],
                    cwd=_SCRIPT_DIR,
                    capture_output=True,
                    text=True,
                )
                if result.returncode != 0:
                    st.error(f"❌ Pipeline échoué (code {result.returncode})")
                    st.code(result.stderr or result.stdout or "Aucun log disponible.")
                else:
                    st.success("✅ Terminé !")
                    time.sleep(1)
                    st.rerun()
            except Exception as exc:
                st.error(f"❌ Erreur inattendue : {exc}")

st.markdown("<hr>", unsafe_allow_html=True)


# ── Data loading ──────────────────────────────────────────────────────────────

def load_data() -> dict[str, pd.DataFrame]:
    xl = pd.ExcelFile(_EXCEL_FILE)
    return {tab: pd.read_excel(xl, sheet) for tab, sheet in _SHEET_MAP.items()}

if not os.path.exists(_EXCEL_FILE):
    st.markdown("""
    <div class="error-box">
        <b>⚠ No strategy file found.</b><br><br>
        <code>Hedge_Fund_Master_Strategy.xlsx</code> does not exist yet.<br>
        Run the full pipeline first using the button above.
    </div>
    """, unsafe_allow_html=True)
    st.stop()

try:
    portfolios = load_data()
except Exception as _load_err:
    st.markdown(f"""
    <div class="error-box">
        <b>⚠ Failed to load strategy file.</b><br><br>
        {_load_err}
    </div>
    """, unsafe_allow_html=True)
    st.stop()


# ── Helpers ───────────────────────────────────────────────────────────────────

def _safe(row: pd.Series, col: str, default=None):
    val = row.get(col, default)
    try:
        if pd.isna(val):
            return default
    except (TypeError, ValueError):
        pass
    return val


def _target_price(row: pd.Series) -> str:
    price = _safe(row, "Last_Price")
    if price is None:
        price = _safe(row, "Current_Price")
    if price is None:
        price = _safe(row, "VWAP")
    mos = _safe(row, "Margin_of_Safety")
    if price is not None:
        if mos is not None and 0 < mos < 1:
            target = price / (1 - mos)
            return f"${target:,.2f}"
        return f"${price * 1.15:,.2f} (+15%)"
    return "N/A"


def _entry_zone(row: pd.Series) -> str:
    price = _safe(row, "Last_Price")
    if price is None:
        price = _safe(row, "Current_Price")
    if price is None:
        price = _safe(row, "VWAP")
    pvwap = _safe(row, "Price_vs_VWAP")
    if price is not None:
        label = f"${price:,.2f}"
        if pvwap is not None:
            pct = pvwap * 100
            sign = "+" if pct >= 0 else ""
            label += f"  ({sign}{pct:.1f}% VWAP)"
        return label
    if pvwap is not None:
        pct = pvwap * 100
        sign = "+" if pct >= 0 else ""
        return f"{sign}{pct:.1f}% vs VWAP"
    return "N/A"


def _var_display(row: pd.Series) -> tuple[str, str]:
    var = _safe(row, "VaR_95")
    if var is None:
        var = _safe(row, "VaR")
    if var is None:
        var = _safe(row, "Ann_Volatility")
    if var is None:
        return "N/A", ""
    pct = abs(float(var)) * 100
    css = "negative" if pct > 20 else ("blue" if pct > 10 else "positive")
    return f"{pct:.1f}%", css


def _mos_display(row: pd.Series) -> tuple[str, str]:
    mos = _safe(row, "Margin_of_Safety")
    if mos is None:
        return "N/A", ""
    pct = mos * 100
    css = "positive" if pct > 20 else ("blue" if pct > 0 else "negative")
    return f"{pct:.1f}%", css


def _metric_html(label: str, value: str, css_class: str = "") -> str:
    return f"""
    <div class="metric-box">
        <div class="metric-label">{label}</div>
        <div class="metric-value {css_class}">{value}</div>
    </div>"""


@st.cache_data(show_spinner=False, ttl=3600)
def _company_name(ticker: str) -> str:
    """Fetches the company long name from yfinance; falls back to ticker."""
    try:
        info = yf.Ticker(ticker).fast_info
        name = getattr(info, "display_name", None) or ticker
        return name
    except Exception:
        return ticker


def _render_stock_card(row: pd.Series) -> None:
    ticker    = _safe(row, "ticker", "—")
    ucs       = _safe(row, "Ultimate_Conviction_Score")
    ucs_str   = f"{ucs:.1f}" if ucs is not None else "—"
    dv_score  = _safe(row, "Deep_Value_Score")
    qr_score  = _safe(row, "Quant_Risk_Score")
    narr      = _safe(row, "Narrative_Score")
    catalysts = _safe(row, "Catalysts", "No data available.")
    threats   = _safe(row, "Threats",   "No data available.")
    ai_impact = _safe(row, "AI_Impact", "No data available.")

    var_str, var_css   = _var_display(row)
    mos_str, mos_css   = _mos_display(row)
    entry_str          = _entry_zone(row)
    target_str         = _target_price(row)

    dv_str  = f"{dv_score:.1f}"  if dv_score  is not None else "N/A"
    qr_str  = f"{qr_score:.1f}" if qr_score  is not None else "N/A"
    narr_str = f"{narr:.0f}/100" if narr      is not None else "N/A"

    name = _company_name(ticker) if ticker != "—" else "—"
    name_display = name if name != ticker else ""

    st.markdown(f"""
    <div class="stock-card">
        <div style="margin-bottom:0.9rem;">
            <span class="ticker-name">{ticker}</span>
            <span class="conviction-badge">CONVICTION {ucs_str}</span>
            {"<br><span style='font-size:0.8rem; color:#90A4AE; letter-spacing:0.05em;'>" + name_display + "</span>" if name_display else ""}
        </div>
        <div style="display:grid; grid-template-columns: repeat(4,1fr); gap:8px; margin-bottom:0.9rem;">
            {_metric_html("ENTRY ZONE", entry_str, "blue")}
            {_metric_html("TARGET PRICE", target_str, "positive")}
            {_metric_html("MARGIN OF SAFETY", mos_str, mos_css)}
            {_metric_html("95% VaR (RISK)", var_str, var_css)}
        </div>
        <div style="display:grid; grid-template-columns: repeat(3,1fr); gap:8px; margin-bottom:0.9rem;">
            {_metric_html("DEEP VALUE SCORE", dv_str)}
            {_metric_html("QUANT RISK SCORE", qr_str)}
            {_metric_html("NARRATIVE SCORE", narr_str)}
        </div>
        <div class="narrative-buy">
            <b>✅ POURQUOI ACHETER (Catalyst):</b><br>{catalysts}
        </div>
        <div class="narrative-risk">
            <b>⚠️ RISQUE &amp; IA:</b><br>
            <b>Threats:</b> {threats}<br>
            <b>AI Impact:</b> {ai_impact}
        </div>
    </div>
    """, unsafe_allow_html=True)


# ── Live Macro Banner ─────────────────────────────────────────────────────────

@st.cache_data(show_spinner=False, ttl=1800)
def _fetch_macro() -> dict:
    import yfinance as yf
    _MACRO_TICKERS = {
        "Crude_Oil":    "CL=F",
        "Gold":         "GC=F",
        "10Y_Treasury": "^TNX",
        "VIX":          "^VIX",
    }
    macro = {}
    for name, symbol in _MACRO_TICKERS.items():
        try:
            data = yf.Ticker(symbol).history(period="5d")
            macro[name] = round(float(data["Close"].iloc[-1]), 2) if not data.empty else None
        except Exception:
            macro[name] = None
    try:
        import requests as _req
        resp = _req.get(
            "https://production.dataviz.cnn.io/index/fearandgreed/graphdata",
            headers={"User-Agent": "Mozilla/5.0", "Referer": "https://www.cnn.com/markets/fear-and-greed"},
            timeout=10,
        )
        fg = resp.json()["fear_and_greed"]
        macro["Fear_Greed_Score"]  = round(float(fg["score"]), 0)
        macro["Fear_Greed_Rating"] = str(fg["rating"]).title()
    except Exception:
        macro["Fear_Greed_Score"]  = None
        macro["Fear_Greed_Rating"] = None
    return macro

def _fg_color(score) -> str:
    if score is None:
        return "#90A4AE"
    if score >= 75:
        return "#4CAF50"
    if score >= 55:
        return "#8BC34A"
    if score >= 45:
        return "#FFC107"
    if score >= 25:
        return "#FF7043"
    return "#F44336"

_macro = _fetch_macro()
_fg_score  = _macro.get("Fear_Greed_Score")
_fg_rating = _macro.get("Fear_Greed_Rating") or "N/A"
_fg_color_val = _fg_color(_fg_score)
_fg_str = f"{int(_fg_score)}/100 — {_fg_rating}" if _fg_score is not None else "N/A"

def _mv(key, unit=""):
    v = _macro.get(key)
    return f"{v:,.2f}{' ' + unit if unit else ''}" if v is not None else "N/A"

st.markdown(f"""
<div style="display:flex; gap:1rem; flex-wrap:wrap; margin-bottom:1.2rem;">
    <div style="flex:1; min-width:140px; background:#0D1B2A; border:1px solid #1E3A5F;
                border-radius:8px; padding:0.7rem 1rem;">
        <div style="font-size:0.65rem; color:#546E7A; letter-spacing:0.1em;">CRUDE OIL (WTI)</div>
        <div style="font-size:1.1rem; font-weight:700; color:#FFF;">{_mv("Crude_Oil")} <span style="font-size:0.7rem;color:#546E7A;">$/bbl</span></div>
    </div>
    <div style="flex:1; min-width:140px; background:#0D1B2A; border:1px solid #1E3A5F;
                border-radius:8px; padding:0.7rem 1rem;">
        <div style="font-size:0.65rem; color:#546E7A; letter-spacing:0.1em;">GOLD</div>
        <div style="font-size:1.1rem; font-weight:700; color:#FFF;">{_mv("Gold")} <span style="font-size:0.7rem;color:#546E7A;">$/oz</span></div>
    </div>
    <div style="flex:1; min-width:140px; background:#0D1B2A; border:1px solid #1E3A5F;
                border-radius:8px; padding:0.7rem 1rem;">
        <div style="font-size:0.65rem; color:#546E7A; letter-spacing:0.1em;">10Y TREASURY</div>
        <div style="font-size:1.1rem; font-weight:700; color:#FFF;">{_mv("10Y_Treasury")} <span style="font-size:0.7rem;color:#546E7A;">%</span></div>
    </div>
    <div style="flex:1; min-width:140px; background:#0D1B2A; border:1px solid #1E3A5F;
                border-radius:8px; padding:0.7rem 1rem;">
        <div style="font-size:0.65rem; color:#546E7A; letter-spacing:0.1em;">VIX</div>
        <div style="font-size:1.1rem; font-weight:700; color:#FFF;">{_mv("VIX")} <span style="font-size:0.7rem;color:#546E7A;">pts</span></div>
    </div>
    <div style="flex:1; min-width:160px; background:#0D1B2A; border:1px solid {_fg_color_val};
                border-radius:8px; padding:0.7rem 1rem;">
        <div style="font-size:0.65rem; color:#546E7A; letter-spacing:0.1em;">FEAR &amp; GREED INDEX</div>
        <div style="font-size:1.1rem; font-weight:700; color:{_fg_color_val};">{_fg_str}</div>
    </div>
</div>
""", unsafe_allow_html=True)


# ── Tabs ──────────────────────────────────────────────────────────────────────

tabs = st.tabs(list(_SHEET_MAP.keys()))

for tab_obj, (tab_label, _) in zip(tabs, _SHEET_MAP.items()):
    with tab_obj:
        df = portfolios[tab_label]
        if df.empty:
            st.markdown('<div class="error-box">No stocks in this portfolio.</div>',
                        unsafe_allow_html=True)
            continue

        for _, row in df.iterrows():
            _render_stock_card(row)
            st.markdown("<hr>", unsafe_allow_html=True)
