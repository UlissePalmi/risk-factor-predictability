from risk_factor_pred.pipeline.steps import (
    step_00_build_universe,
    step_01_download_filings,
    step_02_clean_filings,
    step_03_extract_item1a,
    step_04_compute_features,
    step_05_pull_returns,
    step_06_build_panel,
    step_07_run_models,
)

def main():
    step_00_build_universe()
    step_01_download_filings()
    step_02_clean_filings()
    step_03_extract_item1a()
    step_04_compute_features()
    step_05_pull_returns()
    step_06_build_panel()
    step_07_run_models()

if __name__ == "__main__":
    main()
