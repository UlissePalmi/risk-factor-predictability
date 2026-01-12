from concurrent.futures import ThreadPoolExecutor, as_completed
from risk_factor_pred.consts import FORM, START_DATE, MAX_WORKERS, HTML_DIR, CIK_LIST, TABLES_DIR
import time
import pandas as pd
from . import htmlCleaner
from sec_edgar_downloader import Downloader
import requests

def load_unique_ciks():
    """
    Load a list of CIKs from the Excel file specified in `CIK_LIST`.
    """
    if not CIK_LIST.exists():
        print("Attention! cik_list.csv not downloaded yet.")
        print("Starting cik_list.csv file generation... ")
        cik_list_builder(start_year = 2006 , end_year = 2026)
    df = pd.read_csv(CIK_LIST)
    ciks = df["CIK"].astype(str).str.strip()
    return ciks.tolist()

def download_for_cik(cik: str):
    """
    Download SEC filings for a given CIK using `sec-edgar-downloader`.
    """
    time.sleep(0.1)
    dl = Downloader("MyCompanyName", "my.email@domain.com", str(HTML_DIR))
    print(f"Starting {FORM} for CIK {cik}")
    try:
        dl.get(FORM, cik, after=START_DATE)
        time.sleep(10)
        return cik, "ok", None
    except ValueError as e:
        return cik, "not_found", str(e)
    except Exception as e:
        return cik, "error", str(e)

def workerTasks(cik):
    """
    Execute the per-CIK workflow: download filings and then clean them.

    Steps:
      1) download filings for the CIK (via `download_for_cik`),
      2) zero-pad the CIK to 10 digits,
      3) invoke the HTML cleaning routine to produce a cleaned text file.
    """
    tuple = download_for_cik(cik)
    htmlCleaner.cleaner(str(cik.zfill(10)), output_filename = "full-submission.txt")
    return tuple

def download_n_clean(ciks):
    """
    Download and clean filings for a collection of CIKs using multithreading.

    The function submits one task per CIK to a ThreadPoolExecutor and consumes
    results as tasks finish using `as_completed`. It prints a progress counter
    and summarizes not-found CIKs and errors at the end.
    """
    total = len(ciks)
    print(f"Found {total} unique CIKs")

    not_found = []
    errors = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(workerTasks, cik): cik for cik in ciks}

        # as_completed lets them run in parallel; we consume results as they finish
        for idx, future in enumerate(as_completed(futures), start=1):
            cik, status, err = future.result()
            print(f"[{idx}/{total}] CIK {cik}: {status}")
            if status == "not_found":
                not_found.append(cik)
            elif status == "error":
                errors.append((cik, err))

    if not_found:
        print("\nCIKs not found:")
        for cik in not_found:
            print(" ", cik)

    if errors:

        print("\nCIKs with errors:")
        for cik, err in errors:
            print(f" {cik}: {err}")
    return

def inputLetter():
    """
    Prompt the user to choose between using a saved CIK list or entering a single CIK.

    The function repeatedly prompts until the user enters either:
      - 'l' (use list), or
      - 't' (enter ticker/CIK manually).
    """
    letter = input("Select List (L) or Enter Ticker (T)...").lower()
    while letter != 'l' and letter != 't':
        letter = input("Invalid... enter L or T...").lower()
    return letter




# --------------------------------------------------------------------------------------------------------------------
#                                              CIK LIST BUILDER
# --------------------------------------------------------------------------------------------------------------------


HEADERS = {
    "User-Agent": "Ulisse upalmier@nd.edu",
    "Accept-Encoding": "gzip, deflate",
    "Connection": "keep-alive",
}

def load_master_to_dataframe(year: int, qtr: int) -> pd.DataFrame:
    """
    Download master.idx for a given year/quarter and return it as a DataFrame
    with columns: CIK, Company Name, Form Type, Date Filed, Filename.
    """
    url = f"https://www.sec.gov/Archives/edgar/full-index/{year}/QTR{qtr}/master.idx"
    
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    text = r.text

    lines = text.splitlines()
    records = []

    for line in lines:
        if "|" not in line:
            continue

        parts = line.split("|")
        if len(parts) != 5:
            continue

        cik, name, form_type = parts[:3]

        if form_type != "10-K":
            continue

        records.append(
            {
                "CIK": cik,
                "Company Name": name,
                "Form Type": form_type,
            }
        )
    return pd.DataFrame(records)

def cik_list_builder(start_year, end_year):
    cik_df = []
    for year in range(start_year, end_year):
        for qtr in range(1, 5):
            print(f"Downloading {year}, QTR {qtr}")
            cik_df.append(load_master_to_dataframe(year, qtr))
            
    cik_df = pd.concat(cik_df, ignore_index=True)
    cik_df.sort_values("CIK", inplace = True)

    # Keep only the first row of each consecutive block of equal CIKs
    mask = cik_df["CIK"].astype(str).shift() != cik_df["CIK"].astype(str)

    CIK_LIST = TABLES_DIR / f"cik_list.csv"
    cik_df[mask].to_csv(CIK_LIST, index=False)
