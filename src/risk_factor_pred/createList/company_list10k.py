import requests
import pandas as pd
from pathlib import Path

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


if __name__ == "__main__":
    cik_df = []
    for year in range(2005, 2026):
        for qtr in range(1, 5):
            print(f"Downloading {year}, QTR {qtr}")
            cik_df.append(load_master_to_dataframe(year, qtr))
            

    cik_list = pd.concat(cik_df, ignore_index=True)

    out_dir = Path("cik_list")
    out_dir.mkdir(parents=True, exist_ok=True)

    out_path = out_dir / f"cik_list.csv"
    cik_list.to_csv(out_path, index=False)
    print(f"Saved {len(cik_list)} rows to {out_path}")
