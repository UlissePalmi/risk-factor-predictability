from risk_factor_pred.config import ensure_project_dirs, RAW_EDGAR_DIR, INTERIM_CLEANED_DIR, SIMILARITY_FIELDS, SIMILARITY_FILE, INTERIM_ITEM1A_DIR, FINAL_DATASET, RETURNS_FILE
from risk_factor_pred.edgar import cik_index as cl, downloader as sd
from risk_factor_pred.text import clean as hc, segment as si, tokenize as sm
from risk_factor_pred.wrds import crsp_returns as cr
from risk_factor_pred.datasets import build_panel as bp
from risk_factor_pred.models import rf_setup as rs, rf_classification as rc, rf_regression as rr
import pandas as pd
import csv

def step_00_build_universe():
    
    # Setup data directories
    ensure_project_dirs()

    print("Starting cik_list.csv file generation... ")
    cl.cik_list_builder(start_year = 2006 , end_year = 2026)

def step_01_download_filings():
    
    # Create list of ciks from excel file or request cik in input
    ciks = cl.load_unique_ciks() if cl.inputLetter() == 'l' else [input("Enter CIK...").upper()]
    
    # Download 10-K and Remove HTML tags from previously created list
    sd.download(ciks)

def step_02_clean_filings():
    
    # Create list of ciks from excel file or request cik in input
    ciks = [cik.name for cik in RAW_EDGAR_DIR.iterdir()] if cl.inputLetter() == 'l' else [input("Enter CIK...").zfill(10)]
    
    # Remove HTML tags from each element of previously created list
    [hc.cleaner((cik), output_filename = "full-submission.txt") for cik in ciks]

def step_03_extract_item1a():
    
    # Create list of ciks from excel file or request cik in input
    ciks = [p.name for p in INTERIM_CLEANED_DIR.iterdir()] if cl.inputLetter() == 'l' else [input("Enter ticker...").upper()]

    # 
    si.try_exercize(ciks)

def step_04_compute_features():
    
    # Create list of ciks from excel file or request cik in input
    ciks = [p.name for p in INTERIM_ITEM1A_DIR.iterdir()] if cl.inputLetter() == 'l' else [input("Enter cik...").upper()]
    
    with open(SIMILARITY_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=SIMILARITY_FIELDS)
        writer.writeheader()
        
        sm.concurrency_runner(writer, ciks)
    
def step_05_pull_returns():
    
    return_df = cr.df_with_returns()
    return_df.to_csv(RETURNS_FILE, index=False)

def step_06_build_panel():
  
    sim_df, return_df = bp.datatype_setup(pd.read_csv(SIMILARITY_FILE), pd.read_csv(RETURNS_FILE))

    sim_df = bp.merge_return(sim_df, return_df, months = 18, period = 'future')
    sim_df = bp.merge_return(sim_df, return_df, months = 12, period = 'past')
    
    sim_df.to_csv(FINAL_DATASET, index=False)

def step_07_run_models():
    
    df = pd.read_csv(FINAL_DATASET)

    df = rs.feature_engineering(df)

    # Features X and classification target y
    df_cat, labels = rc.create_labels(df, prediction_col = "future_18m_ret")
    X, y = rs.X_y_builder(df_cat)
    rc.rf_cat(X, y, labels)

    df["prediction"] = df["future_18m_ret"]
    X, y = rs.X_y_builder(df)
    rr.rf_reg(X, y, df)
