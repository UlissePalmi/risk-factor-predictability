import pandas as pd
from risk_factor_pred.consts import SEC_DIR, TABLES_DIR
import re

def find_year(pname):
    pattern = re.compile(r"^\d{10}-(\d{2})-\d{6}$")
    m = pattern.match(pname)
    yy = int(m.group(1))
    year = 1900 + yy if yy >= 70 else 2000 + yy
    return year

df = pd.DataFrame(columns=['cik', 'year'])

similarity_df = pd.read_csv(TABLES_DIR / 'similarity.csv')
similarity_df['cik'] = similarity_df['cik'].astype(int).map(lambda n: f"{n:010d}")

for path in SEC_DIR.iterdir():
    folder = path / "10-K"
    cik_df = similarity_df[similarity_df['cik'] == path.name]
    
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