from risk_factor_pred.config import ensure_project_dirs, RAW_EDGAR_DIR, INTERIM_CLEANED_DIR, FEATURES_FIELDS, FEATURES_FILE, INTERIM_ITEM1A_DIR, FINAL_DATASET, RETURNS_FILE, CIK_LIST
from risk_factor_pred.edgar import cik_index as cl, downloader as sd
from risk_factor_pred.text import clean as hc, segment as si, tokenize as sm
from risk_factor_pred.wrds import crsp_returns as cr
from risk_factor_pred.datasets import build_panel as bp
from risk_factor_pred.models import rf_setup as rs, rf_classification as rc, rf_regression as rr
from typing import Iterable, List, Optional
from pathlib import Path
import pandas as pd
import argparse
import csv

def _digits_only(x: str) -> str:
    return "".join(ch for ch in x if ch.isdigit())

def _resolve_cik_dirs(base_dir: Path, ciks: Optional[Iterable[str]]) -> List[str]:
    """
    Resolve which CIK directory names to process under `base_dir`.
    - If ciks is None: return all subdirectory names.
    - If provided: try both padded/unpadded representations and pick the one that exists.
    """
    if ciks is None:
        return sorted([p.name for p in base_dir.iterdir() if p.is_dir()])
    print(ciks)
    resolved = []
    for cik in ciks:
        raw = str(cik).strip()
        digits = _digits_only(raw)

        candidates = []
        if digits:
            candidates.extend([digits, digits.zfill(10), digits.lstrip("0") or digits])
        else:
            candidates.append(raw)

        picked = None
        for cand in dict.fromkeys(candidates):  # unique, preserve order
            if (base_dir / cand).exists():
                picked = cand
                break

        # Fallback: if nothing exists yet (e.g., first run), keep padded form for consistency
        if picked is None:
            picked = digits.zfill(10) if digits else raw

        resolved.append(picked)
    return resolved
    
def _parse_args():
    """
    Parse CLI arguments for running the end-to-end pipeline.

    Supports selecting a single CIK or a comma-separated list, year range,
    and a step interval to run.
    """
    p = argparse.ArgumentParser(
        description="Reproduce the full risk-factor predictability pipeline."
    )

    g = p.add_mutually_exclusive_group()
    g.add_argument("--cik", type=str, help="Single CIK (digits). Example: 320193")
    g.add_argument("--ciks", type=str, help="Comma-separated CIKs. Example: 320193,789019")

    p.add_argument("--start-year", type=int, default=2006)
    p.add_argument("--end-year", type=int, default=2026)

    p.add_argument("--from-step", type=int, default=0, choices=range(0, 8))
    p.add_argument("--to-step", type=int, default=7, choices=range(0, 8))

    return p.parse_args()

def step_00_build_universe(start_year: int = 2006 , end_year: int = 2026) -> None:
    """
    Create the CIK universe used for the pipeline.

    Builds `cik_list.csv` from SEC index files if it does not already exist.
    """
    ensure_project_dirs()
    print("Starting cik_list.csv file generation... ")
    if not CIK_LIST.exists():
        cl.cik_list_builder(start_year, end_year)
    else:
        print("CIK_LIST already exists")

def step_01_download_filings(ciks: Optional[Iterable[str]] = None):
    """
    Download raw SEC filings for the requested CIKs.

    If `ciks` is None, uses the full universe from `cik_list.csv`.
    """
    if ciks is None:
        ciks = cl.load_unique_ciks()
    sd.download(ciks)

def step_02_clean_filings(ciks: Optional[Iterable[str]] = None) -> None:
    """
    Clean downloaded SEC filings into standardized text files.

    If `ciks` is None, processes all CIK folders found in the raw directory.
    """
    print(ciks)
    ciks_dirs = _resolve_cik_dirs(RAW_EDGAR_DIR, ciks)
    hc.clean_worker(ciks_dirs)

def step_03_extract_item1a(ciks: Optional[Iterable[str]] = None) -> None:
    """
    Extract Item 1A risk factor text from cleaned filings.

    If `ciks` is None, processes all CIK folders found in the cleaned directory.
    """
    ciks_dirs = _resolve_cik_dirs(INTERIM_CLEANED_DIR, ciks)
    si.try_exercize(ciks_dirs)

def step_04_compute_features(ciks: Optional[Iterable[str]] = None) -> None:
    """
    Compute levenshtein/sentiment features from extracted Item 1A text.

    Writes row-level results into `FEATURES_FILE`.
    """
    ciks_dirs = _resolve_cik_dirs(INTERIM_ITEM1A_DIR, ciks)

    with open(FEATURES_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FEATURES_FIELDS)
        writer.writeheader()
        sm.concurrency_runner(writer, ciks_dirs)
    
def step_05_pull_returns() -> None:
    """
    Pull monthly return data from WRDS/CRSP for the CIK universe.

    Saves the combined return panel to `RETURNS_FILE`.
    """
    return_df = cr.df_with_returns()
    return_df.to_csv(RETURNS_FILE, index=False)
    old_ciks_df = pd.read_csv(CIK_LIST)
    old_ciks_df[old_ciks_df['CIK']==return_df['CIK']]
    print(old_ciks_df)

def step_06_build_panel() -> None:
    """
    Merge text features with returns to create the final modeling dataset.

    Produces `FINAL_DATASET` with past/future window returns added.
    """
    sim_df, return_df = bp.datatype_setup(pd.read_csv(FEATURES_FILE), pd.read_csv(RETURNS_FILE))
    print(sim_df)
    sim_df = bp.merge_return(sim_df, return_df, months=18, period="future")
    sim_df = bp.merge_return(sim_df, return_df, months=12, period="past")
    
    sim_df.to_csv(FINAL_DATASET, index=False)

def step_07_run_models() -> None:
    """
    Run the classification and regression models on the final dataset.

    Trains the Random Forest models and prints evaluation output.
    """
    df = pd.read_csv(FINAL_DATASET)

    df = rs.feature_engineering(df)

    df_cat, labels = rc.create_labels(df, prediction_col="future_18m_ret")
    print(df_cat)
    X, y = rs.X_y_builder(df_cat)
    rc.rf_cat(X, y, labels)

    df["prediction"] = df["future_18m_ret"]
    X, y = rs.X_y_builder(df)
    rr.rf_reg(X, y, df)