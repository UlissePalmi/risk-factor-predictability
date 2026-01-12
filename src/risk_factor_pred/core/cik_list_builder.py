from risk_factor_pred.config import CIK_LIST, TABLES_DIR
import pandas as pd
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