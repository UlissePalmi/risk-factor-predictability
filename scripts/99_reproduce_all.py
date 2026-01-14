from __future__ import annotations
from risk_factor_pred.config import ensure_project_dirs, CIK_LIST
from risk_factor_pred.edgar.cik_index import _load_ciks
from risk_factor_pred.pipeline import steps as s

"""
Entry point for reproducing the full pipeline end-to-end.

Runs the SEC download → cleaning → Item 1A extraction → feature construction →
returns merge → panel build → model estimation steps, with optional step ranges
controlled by CLI arguments.
"""

def main():
    args = s._parse_args()
    ensure_project_dirs()
    ciks = _load_ciks(args)

    steps = {
        0: ("build_universe", lambda: s.step_00_build_universe(args.start_year, args.end_year)),
        1: ("download_filings", lambda: s.step_01_download_filings(ciks)),
        2: ("clean_filings", lambda: s.step_02_clean_filings(ciks)),
        3: ("extract_item1a", lambda: s.step_03_extract_item1a(ciks)),
        4: ("compute_features", lambda: s.step_04_compute_features(ciks)),
        5: ("pull_returns", s.step_05_pull_returns),
        6: ("build_panel", s.step_06_build_panel),
        7: ("run_models", s.step_07_run_models),
    }

    print(f"Running pipeline for: {('ALL CIKs' if ciks is None else f'{len(ciks)} CIK(s)')}")

    if args.from_step > args.to_step:
        raise ValueError("--from-step must be <= --to-step")

    for i in range(args.from_step, args.to_step + 1):
        name, fn = steps[i]
        print(f"\n=== Step {i}: {name} ===")
        try:
            fn()
        except Exception as e:
            raise RuntimeError(f"Failed at step {i}: {name}") from e

    print("\nDone.")

if __name__ == "__main__":
    main()
