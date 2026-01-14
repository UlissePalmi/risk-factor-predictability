import pandas as pd
from risk_factor_pred.config import SEC_DIR, TABLES_DIR
import re

"""
This script audits missing filing years in the features dataset.

For each CIK folder under `SEC_DIR`, it extracts filing years from the accession
folder names and compares them to the years covered by `features.csv`. Any
filing years that are present on disk but missing from the features output
are collected and saved to `missing_years.csv`.
"""

def find_year(pname):
    """
    Extract the filing year from a SEC accession string (YY in the middle field).

    Converts two-digit years to a four-digit year using a 1970 cutoff rule.
    """
    pattern = re.compile(r"^\d{10}-(\d{2})-\d{6}$")
    m = pattern.match(pname)
    yy = int(m.group(1))
    year = 1900 + yy if yy >= 70 else 2000 + yy
    return year

df = pd.DataFrame(columns=['cik', 'year'])

features_df = pd.read_csv(TABLES_DIR / 'features.csv')
features_df['cik'] = features_df['cik'].astype(int).map(lambda n: f"{n:010d}")

for path in SEC_DIR.iterdir():
    folder = path / "10-K"
    cik_df = features_df[features_df['cik'] == path.name]
    
    if cik_df.empty:
        years = [find_year(p.name) for p in folder.iterdir()]
    else:
        a = cik_df["date_a"].tolist()
        a.append(cik_df["date_b"].iloc[-1])
        a = [i[:4] for i in a]
        
        # p.name is the cik
        years = [str(find_year(p.name)) for p in folder.iterdir() if not str(find_year(p.name)) in a]
        
    rows = [[path.name, year] for year in years]
    for row in rows:
        df.loc[len(df)] = row

print(df)

df.to_csv(TABLES_DIR / "missing_years.csv")