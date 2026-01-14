from __future__ import annotations
from risk_factor_pred.config import ensure_project_dirs, CIK_LIST
from risk_factor_pred.edgar.cik_index import _load_ciks
from risk_factor_pred.pipeline.steps import (
    step_00_build_universe,
    step_01_download_filings,
    step_02_clean_filings,
    step_03_extract_item1a,
    step_04_compute_features,
    step_05_pull_returns,
    step_06_build_panel,
    step_07_run_models,
    _parse_args
)

def main():
    args = _parse_args()
    ensure_project_dirs()
    ciks = _load_ciks(args)

    steps = {
        0: ("build_universe", lambda: step_00_build_universe(args.start_year, args.end_year)),
        1: ("download_filings", lambda: step_01_download_filings(ciks)),
        2: ("clean_filings", lambda: step_02_clean_filings(ciks)),
        3: ("extract_item1a", lambda: step_03_extract_item1a(ciks)),
        4: ("compute_features", lambda: step_04_compute_features(ciks)),
        5: ("pull_returns", step_05_pull_returns),
        6: ("build_panel", step_06_build_panel),
        7: ("run_models", step_07_run_models),
    }

    if args.from_step > args.to_step:
        raise ValueError("--from-step must be <= --to-step")

    for i in range(args.from_step, args.to_step + 1):
        name, fn = steps[i]
        print(f"\n=== Step {i}: {name} ===")
        fn()

    print("\nDone.")

if __name__ == "__main__":
    main()
