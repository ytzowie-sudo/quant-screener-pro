import subprocess
import sys
import time

_PIPELINE = [
    ("01_macro_and_universe.py",    "Macro & Universe   → global_universe.csv"),
    ("01_data_loader.py",           "Data Loader        → data_loaded.csv"),
    ("02_fundamentals.py",          "Fundamentals        → fundamentals.csv"),
    ("02_deep_valuation.py",        "Deep Valuation      → deep_valuation.csv"),
    ("03_technicals.py",            "Technicals          → technicals.csv"),
    ("03_quant_risk_models.py",     "Quant Risk Models   → quant_risk.csv"),
    ("04_sentiment_and_export.py",  "FinBERT Sentiment   → sentiment.csv"),
    ("04_event_driven.py",          "Event-Driven Track  → event_driven.csv"),
    ("04_perplexity_narrative.py",  "Perplexity AI       → ai_narrative.csv"),
    ("05_portfolio_allocator.py",   "Portfolio Allocator → Excel"),
]

_TOTAL = len(_PIPELINE)


def _banner() -> None:
    print()
    print("=" * 65)
    print("  ██████  ██    ██  █████  ███    ██ ████████")
    print(" ██    ██ ██    ██ ██   ██ ████   ██    ██   ")
    print(" ██    ██ ██    ██ ███████ ██ ██  ██    ██   ")
    print(" ██ ▄▄ ██ ██    ██ ██   ██ ██  ██ ██    ██   ")
    print("  ██████   ██████  ██   ██ ██   ████    ██   ")
    print("     ▀▀                                      ")
    print()
    print("       SCREENER PRO  V3.0  —  HYBRID ENGINE")
    print("       Institutional-Grade Quantitative Fund")
    print("=" * 65)
    print()


def _step_header(index: int, script: str, label: str) -> None:
    print()
    print(f"  ┌─────────────────────────────────────────────────────┐")
    print(f"  │  [{index}/{_TOTAL}]  {label:<47}│")
    print(f"  │        {script:<51}│")
    print(f"  └─────────────────────────────────────────────────────┘")


def _success_banner(elapsed: float) -> None:
    minutes, seconds = divmod(int(elapsed), 60)
    print()
    print("=" * 65)
    print()
    print("  ✅  ALL 10 PIPELINE STAGES COMPLETED SUCCESSFULLY")
    print()
    print(f"  📁  Output  →  Hedge_Fund_Master_Strategy.xlsx")
    print(f"  ⏱   Runtime →  {minutes}m {seconds}s")
    print()
    print("  Your institutional hedge fund strategy is ready.")
    print("  Open Hedge_Fund_Master_Strategy.xlsx to review:")
    print("    • Court Terme  (Catalysts)  — Short-term plays")
    print("    • Moyen Terme  (Momentum)   — Mid-term VWAP surfers")
    print("    • Long Terme   (Value)      — Buffett-style holds")
    print()
    print("=" * 65)
    print()


def main() -> None:
    _banner()
    start_time = time.time()

    for idx, (script, label) in enumerate(_PIPELINE, start=1):
        _step_header(idx, script, label)
        step_start = time.time()
        try:
            subprocess.run(
                [sys.executable, script],
                check=True,
            )
            step_elapsed = time.time() - step_start
            print(f"\n  ✔  Done in {step_elapsed:.1f}s\n")

        except FileNotFoundError:
            print()
            print(f"  ╔══════════════════════════════════════════════════╗")
            print(f"  ║  CRITICAL ERROR — Script not found               ║")
            print(f"  ║  Missing file: {script:<35}║")
            print(f"  ║  Pipeline halted at step [{idx}/{_TOTAL}].                  ║")
            print(f"  ╚══════════════════════════════════════════════════╝")
            print()
            sys.exit(1)

        except subprocess.CalledProcessError as exc:
            print()
            print(f"  ╔══════════════════════════════════════════════════╗")
            print(f"  ║  CRITICAL ERROR — Script failed                  ║")
            print(f"  ║  Script  : {script:<39}║")
            print(f"  ║  Exit code: {exc.returncode:<38}║")
            print(f"  ║  Pipeline halted at step [{idx}/{_TOTAL}].                  ║")
            print(f"  ╚══════════════════════════════════════════════════╝")
            print()
            sys.exit(1)

    _success_banner(time.time() - start_time)


if __name__ == "__main__":
    main()
