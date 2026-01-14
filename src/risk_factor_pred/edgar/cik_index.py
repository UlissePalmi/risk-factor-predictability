from risk_factor_pred.config import CIK_LIST, RAW_CIKS_DIR
import pandas as pd
import requests

def load_unique_ciks():
    """
    Load a list of CIKs from the Excel file specified in `CIK_LIST`.
    """
    df = pd.read_csv(CIK_LIST)
    ciks = df["CIK"].astype(str).str.strip()
    return ciks.tolist()

def _load_ciks(args):
    if args.cik:
        return [args.cik.strip()]
    elif args.ciks:
        return [x.strip() for x in args.ciks.split(",") if x.strip()]
    else:
        return None

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

def load_master_to_dataframe(year: int, qtr: int) -> pd.DataFrame:
    """
    Download master.idx for a given year/quarter and return it as a DataFrame
    with columns: CIK, Company Name, Form Type.
    """
    url = f"https://www.sec.gov/Archives/edgar/full-index/{year}/QTR{qtr}/master.idx"
    
    HEADERS = {
    "User-Agent": "Name name@domain.com",
    "Accept-Encoding": "gzip, deflate",
    "Connection": "keep-alive",
    }

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
    """
    Build and save a master list of unique 10-K CIKs over a range of years.

    For each year in [start_year, end_year) and each quarter (1-4), the function
    downloads the SEC `master.idx` index, filters for 10-K filings, concatenates
    all results, removes duplicate CIKs, and writes the final list to
    `RAW_CIKS_DIR / "cik_list.csv"`.
    """
    cik_df = []
    for year in range(start_year, end_year):
        for qtr in range(1, 5):
            print(f"Downloading {year}, QTR {qtr}")
            cik_df.append(load_master_to_dataframe(year, qtr))
            
    cik_df = pd.concat(cik_df, ignore_index=True)
    cik_df.sort_values("CIK", inplace = True)

    # Keep only the first row of each consecutive block of equal CIKs
    mask = cik_df["CIK"].astype(str).shift() != cik_df["CIK"].astype(str)

    CIK_LIST = RAW_CIKS_DIR / "cik_list.csv"
    cik_df[mask].to_csv(CIK_LIST, index=False)