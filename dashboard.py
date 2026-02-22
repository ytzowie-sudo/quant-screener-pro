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
    for attempt in [col, col + "_y", col + "_x"]:
        val = row.get(attempt, None)
        if val is None:
            continue
        try:
            if pd.isna(val):
                continue
        except (TypeError, ValueError):
            pass
        return val
    return default


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
    _color_map = {
        "positive": "#00C853",
        "negative": "#FF1744",
        "blue":     "#1E88E5",
        "":         "#E8EAF0",
    }
    color = _color_map.get(css_class, "#E8EAF0")
    return (
        '<div style="background:#13151F; border:1px solid #1A2340; border-radius:3px;'
        ' padding:0.6rem 0.9rem; text-align:center;">'
        '<div style="font-size:0.65rem; color:#5C6A8A; letter-spacing:0.1em;'
        ' text-transform:uppercase; margin-bottom:4px;">' + label + "</div>"
        '<div style="font-size:1.05rem; font-weight:700; color:' + color + ';">' + value + "</div>"
        "</div>"
    )


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
def _fetch_sector_rotation() -> list:
    _SECTORS = {
        "Tech":        "XLK",
        "Finance":     "XLF",
        "Health":      "XLV",
        "Energy":      "XLE",
        "Consumer":    "XLY",
        "Industrials": "XLI",
        "Materials":   "XLB",
        "Utilities":   "XLU",
        "Real Estate": "XLRE",
        "Staples":     "XLP",
        "Comm Svcs":   "XLC",
    }
    results = []
    for sector, symbol in _SECTORS.items():
        try:
            hist = yf.Ticker(symbol).history(period="3mo")
            if hist.empty or len(hist) < 5:
                continue
            p_now = float(hist["Close"].iloc[-1])
            p_1m  = float(hist["Close"].iloc[-22]) if len(hist) >= 22 else p_now
            p_1w  = float(hist["Close"].iloc[-6])  if len(hist) >= 6  else p_now
            results.append({
                "sector": sector,
                "1W": round((p_now - p_1w) / p_1w * 100, 2),
                "1M": round((p_now - p_1m) / p_1m * 100, 2),
            })
        except Exception:
            continue
    results.sort(key=lambda x: x["1M"], reverse=True)
    return results


@st.cache_data(show_spinner=False, ttl=1800)
def _fetch_capital_flows() -> dict:
    _FLOW_ETFS = {
        "🇺🇸 US":       "SPY",
        "🇪🇺 Europe":   "VGK",
        "🇯🇵 Japan":    "EWJ",
        "🌏 Emerging":  "EEM",
        "🇨🇳 China":    "FXI",
        "💵 DXY":       "DX-Y.NYB",
    }
    flows = {}
    perf  = {}
    for region, symbol in _FLOW_ETFS.items():
        try:
            hist = yf.Ticker(symbol).history(period="3mo")
            if hist.empty or len(hist) < 5:
                flows[region] = {"1W": None, "1M": None, "3M": None}
                continue
            p_now = float(hist["Close"].iloc[-1])
            p_1w  = float(hist["Close"].iloc[-6])  if len(hist) >= 6  else p_now
            p_1m  = float(hist["Close"].iloc[-22]) if len(hist) >= 22 else p_now
            p_3m  = float(hist["Close"].iloc[0])
            flows[region] = {
                "1W": round((p_now - p_1w) / p_1w * 100, 2),
                "1M": round((p_now - p_1m) / p_1m * 100, 2),
                "3M": round((p_now - p_3m) / p_3m * 100, 2),
            }
            if "DXY" not in region:
                perf[region] = flows[region]["1M"]
        except Exception:
            flows[region] = {"1W": None, "1M": None, "3M": None}
    dominant = max(perf, key=perf.get) if perf else None
    weakest  = min(perf, key=perf.get) if perf else None
    return {"flows": flows, "dominant": dominant, "weakest": weakest}


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

    _FRED_BASE = "https://fred.stlouisfed.org/graph/fredgraph.csv?id="
    _FRED_SERIES = {"CPI_YoY": "CPIAUCSL", "Unemployment": "UNRATE",
                    "Fed_Funds_Rate": "FEDFUNDS", "Yield_Curve": "T10Y2Y"}
    for fname, fid in _FRED_SERIES.items():
        try:
            import requests as _req2
            r = _req2.get(f"{_FRED_BASE}{fid}", timeout=10)
            lines = r.text.strip().split("\n")
            last_val = float(lines[-1].split(",")[1])
            if fname == "CPI_YoY" and len(lines) > 13:
                val_12m = float(lines[-13].split(",")[1])
                macro[fname] = round((last_val - val_12m) / val_12m * 100, 2)
            else:
                macro[fname] = round(last_val, 3)
        except Exception:
            macro[fname] = None

    yc = macro.get("Yield_Curve")
    if yc is not None:
        macro["Recession_Signal"] = "⚠️ INVERTED" if yc < 0 else ("🟡 FLAT" if yc < 0.5 else "✅ NORMAL")
    else:
        macro["Recession_Signal"] = "N/A"

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

import datetime as _dt
_benner = {
    "Benner_Year":        _dt.datetime.now().year,
    "Benner_Phase":       "GOOD TIMES — Approche du sommet",
    "Benner_Signal":      "REDUCE/CAUTION",
    "Benner_Color":       "orange",
    "Benner_Next_Panic":  2036,
    "Benner_Next_Boom":   2026,
    "Benner_Yrs_To_Panic": 10,
    "Benner_Yrs_To_Boom":  0,
}
try:
    import sys as _sys, importlib.util as _ilu
    _spec = _ilu.spec_from_file_location("macro_mod", os.path.join(_SCRIPT_DIR, "01_macro_and_universe.py"))
    _macro_mod = _ilu.module_from_spec(_spec)
    _spec.loader.exec_module(_macro_mod)
    _benner = _macro_mod.benner_cycle_phase()
except Exception:
    pass

_bn_phase  = _benner.get("Benner_Phase", "N/A")
_bn_signal = _benner.get("Benner_Signal", "")
_bn_boom   = _benner.get("Benner_Next_Boom")
_bn_panic  = _benner.get("Benner_Next_Panic")
_bn_color_map = {"green": "#4CAF50", "red": "#F44336", "orange": "#FF9800", "blue": "#1E88E5", "grey": "#90A4AE"}
_bn_color  = _bn_color_map.get(_benner.get("Benner_Color", "grey"), "#90A4AE")
_bn_sub    = f"Prochain Boom: {_bn_boom} · Prochain Panic: {_bn_panic}" if _bn_boom and _bn_panic else ""

st.markdown(f"""
<div style="display:flex; gap:1rem; flex-wrap:wrap; margin-bottom:1.2rem;">
    <div style="flex:1; min-width:130px; background:#0D1B2A; border:1px solid #1E3A5F;
                border-radius:8px; padding:0.7rem 1rem;">
        <div style="font-size:0.65rem; color:#546E7A; letter-spacing:0.1em;">CRUDE OIL (WTI)</div>
        <div style="font-size:1.1rem; font-weight:700; color:#FFF;">{_mv("Crude_Oil")} <span style="font-size:0.7rem;color:#546E7A;">$/bbl</span></div>
    </div>
    <div style="flex:1; min-width:130px; background:#0D1B2A; border:1px solid #1E3A5F;
                border-radius:8px; padding:0.7rem 1rem;">
        <div style="font-size:0.65rem; color:#546E7A; letter-spacing:0.1em;">GOLD</div>
        <div style="font-size:1.1rem; font-weight:700; color:#FFF;">{_mv("Gold")} <span style="font-size:0.7rem;color:#546E7A;">$/oz</span></div>
    </div>
    <div style="flex:1; min-width:130px; background:#0D1B2A; border:1px solid #1E3A5F;
                border-radius:8px; padding:0.7rem 1rem;">
        <div style="font-size:0.65rem; color:#546E7A; letter-spacing:0.1em;">10Y TREASURY</div>
        <div style="font-size:1.1rem; font-weight:700; color:#FFF;">{_mv("10Y_Treasury")} <span style="font-size:0.7rem;color:#546E7A;">%</span></div>
    </div>
    <div style="flex:1; min-width:130px; background:#0D1B2A; border:1px solid #1E3A5F;
                border-radius:8px; padding:0.7rem 1rem;">
        <div style="font-size:0.65rem; color:#546E7A; letter-spacing:0.1em;">VIX</div>
        <div style="font-size:1.1rem; font-weight:700; color:#FFF;">{_mv("VIX")} <span style="font-size:0.7rem;color:#546E7A;">pts</span></div>
    </div>
    <div style="flex:1; min-width:150px; background:#0D1B2A; border:1px solid {_fg_color_val};
                border-radius:8px; padding:0.7rem 1rem;">
        <div style="font-size:0.65rem; color:#546E7A; letter-spacing:0.1em;">FEAR &amp; GREED INDEX</div>
        <div style="font-size:1.1rem; font-weight:700; color:{_fg_color_val};">{_fg_str}</div>
    </div>
    <div style="flex:2; min-width:220px; background:#0D1B2A; border:1px solid {_bn_color};
                border-radius:8px; padding:0.7rem 1rem;">
        <div style="font-size:0.65rem; color:#546E7A; letter-spacing:0.1em;">BENNER CYCLE</div>
        <div style="font-size:0.95rem; font-weight:700; color:{_bn_color};">{_bn_phase}</div>
        <div style="font-size:0.7rem; color:#546E7A; margin-top:2px;">{_bn_sub}</div>
    </div>
</div>
""", unsafe_allow_html=True)

_fred_cpi  = _macro.get("CPI_YoY")
_fred_unem = _macro.get("Unemployment")
_fred_fed  = _macro.get("Fed_Funds_Rate")
_fred_yc   = _macro.get("Yield_Curve")
_fred_rec  = _macro.get("Recession_Signal", "N/A")
_fred_yc_color = "#F44336" if _fred_yc is not None and _fred_yc < 0 else ("#FFC107" if _fred_yc is not None and _fred_yc < 0.5 else "#4CAF50")

def _fv(v, suffix=""):
    return f"{v}{suffix}" if v is not None else "N/A"

st.markdown(f"""
<div style="display:flex; gap:0.7rem; flex-wrap:wrap; margin-bottom:1.2rem;">
    <div style="flex:1; min-width:120px; background:#0D1B2A; border:1px solid #1E3A5F;
                border-radius:8px; padding:0.6rem 0.9rem;">
        <div style="font-size:0.6rem; color:#546E7A; letter-spacing:0.1em;">CPI INFLATION YoY</div>
        <div style="font-size:1rem; font-weight:700; color:#FFF;">{_fv(_fred_cpi, "%")}</div>
    </div>
    <div style="flex:1; min-width:120px; background:#0D1B2A; border:1px solid #1E3A5F;
                border-radius:8px; padding:0.6rem 0.9rem;">
        <div style="font-size:0.6rem; color:#546E7A; letter-spacing:0.1em;">CHÔMAGE US</div>
        <div style="font-size:1rem; font-weight:700; color:#FFF;">{_fv(_fred_unem, "%")}</div>
    </div>
    <div style="flex:1; min-width:120px; background:#0D1B2A; border:1px solid #1E3A5F;
                border-radius:8px; padding:0.6rem 0.9rem;">
        <div style="font-size:0.6rem; color:#546E7A; letter-spacing:0.1em;">FED FUNDS RATE</div>
        <div style="font-size:1rem; font-weight:700; color:#FFF;">{_fv(_fred_fed, "%")}</div>
    </div>
    <div style="flex:1; min-width:120px; background:#0D1B2A; border:1px solid {_fred_yc_color};
                border-radius:8px; padding:0.6rem 0.9rem;">
        <div style="font-size:0.6rem; color:#546E7A; letter-spacing:0.1em;">YIELD CURVE (10Y-2Y)</div>
        <div style="font-size:1rem; font-weight:700; color:{_fred_yc_color};">{_fv(_fred_yc)}</div>
    </div>
    <div style="flex:2; min-width:180px; background:#0D1B2A; border:1px solid {_fred_yc_color};
                border-radius:8px; padding:0.6rem 0.9rem;">
        <div style="font-size:0.6rem; color:#546E7A; letter-spacing:0.1em;">SIGNAL RÉCESSION (FRED)</div>
        <div style="font-size:0.95rem; font-weight:700; color:{_fred_yc_color};">{_fred_rec}</div>
    </div>
</div>
""", unsafe_allow_html=True)


# ── Capital Flows Banner ───────────────────────────────────────────────────────

_cf = _fetch_capital_flows()
_cf_flows    = _cf.get("flows", {})
_cf_dominant = _cf.get("dominant")
_cf_weakest  = _cf.get("weakest")

def _pct_html(val):
    if val is None:
        return '<span style="color:#546E7A">N/A</span>'
    color = "#4CAF50" if val >= 0 else "#F44336"
    arrow = "▲" if val >= 0 else "▼"
    return f'<span style="color:{color}">{arrow} {abs(val):.1f}%</span>'

_cf_cards = ""
for region, data in _cf_flows.items():
    is_dominant = region == _cf_dominant
    is_weakest  = region == _cf_weakest
    border_color = "#4CAF50" if is_dominant else ("#F44336" if is_weakest else "#1E3A5F")
    badge = " 🏆" if is_dominant else (" ⚠️" if is_weakest else "")
    w1 = _pct_html(data.get("1W"))
    m1 = _pct_html(data.get("1M"))
    m3 = _pct_html(data.get("3M"))
    _cf_cards += (
        f'<div style="flex:1; min-width:130px; background:#0D1B2A; border:1px solid {border_color};'
        f'border-radius:8px; padding:0.6rem 0.9rem;">'
        f'<div style="font-size:0.7rem; font-weight:700; color:#CFD8DC; margin-bottom:4px;">{region}{badge}</div>'
        f'<div style="font-size:0.68rem; color:#546E7A;">1W: {w1}</div>'
        f'<div style="font-size:0.68rem; color:#546E7A;">1M: {m1}</div>'
        f'<div style="font-size:0.68rem; color:#546E7A;">3M: {m3}</div>'
        f'</div>'
    )

_cf_signal = ""
if _cf_dominant and _cf_weakest:
    _cf_signal = f"💰 Capitaux vers <b>{_cf_dominant}</b> · Fuite de <b>{_cf_weakest}</b> (sur 1 mois)"

st.markdown(f"""
<div style="margin-bottom:0.4rem;">
    <span style="font-size:0.65rem; color:#546E7A; letter-spacing:0.12em;">
        FLUX DE CAPITAUX RÉGIONAUX (ETF PROXY) &nbsp;·&nbsp; {_cf_signal}
    </span>
</div>
<div style="display:flex; gap:0.7rem; flex-wrap:wrap; margin-bottom:1.4rem;">
    {_cf_cards}
</div>
""", unsafe_allow_html=True)


# ── Sector Rotation ────────────────────────────────────────────────────────────

_sectors = _fetch_sector_rotation()
if _sectors:
    _top3    = [s["sector"] for s in _sectors[:3]]
    _bot3    = [s["sector"] for s in _sectors[-3:]]
    _sec_cards = ""
    for s in _sectors:
        _1m = s["1M"]
        _1w = s["1W"]
        _is_top = s["sector"] in _top3
        _is_bot = s["sector"] in _bot3
        _border = "#4CAF50" if _is_top else ("#F44336" if _is_bot else "#1E3A5F")
        _c1m = "#4CAF50" if _1m >= 0 else "#F44336"
        _c1w = "#4CAF50" if _1w >= 0 else "#F44336"
        _a1m = "▲" if _1m >= 0 else "▼"
        _a1w = "▲" if _1w >= 0 else "▼"
        _sname = s["sector"]
        _sec_cards += (
            f'<div style="flex:1; min-width:100px; background:#0D1B2A; border:1px solid {_border};'
            f'border-radius:8px; padding:0.5rem 0.8rem; text-align:center;">'
            f'<div style="font-size:0.68rem; font-weight:700; color:#CFD8DC;">{_sname}</div>'
            f'<div style="font-size:0.65rem; color:{_c1m};">{_a1m} {abs(_1m):.1f}% <span style="color:#546E7A">1M</span></div>'
            f'<div style="font-size:0.65rem; color:{_c1w};">{_a1w} {abs(_1w):.1f}% <span style="color:#546E7A">1W</span></div>'
            f'</div>'
        )

    st.markdown(f"""
<div style="margin-bottom:0.4rem;">
    <span style="font-size:0.65rem; color:#546E7A; letter-spacing:0.12em;">
        ROTATION SECTORIELLE (US) &nbsp;·&nbsp;
        🏆 <b>{" · ".join(_top3)}</b> &nbsp;|&nbsp;
        ⚠️ <b>{" · ".join(_bot3)}</b>
    </span>
</div>
<div style="display:flex; gap:0.5rem; flex-wrap:wrap; margin-bottom:1.6rem;">
    {_sec_cards}
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
