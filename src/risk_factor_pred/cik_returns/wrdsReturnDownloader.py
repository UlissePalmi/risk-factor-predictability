import wrds
import pandas as pd
from risk_factor_pred.consts import CIK_LIST, TABLES_DIR

def querymaker(cik):
    query = f"""
    SELECT 
        c.cik, 
        c.conm as company_name, 
        m.date, 
        m.ret
    FROM 
        crsp.msf as m
    JOIN 
        crsp.ccmxpf_linktable as link
        ON m.permno = link.lpermno
    JOIN
        comp.company as c
        ON link.gvkey = c.gvkey
    WHERE 
        c.cik = '{cik}'            
        AND m.date >= '2006-01-01'      
        AND m.date <= '2025-10-31'
        AND link.linktype IN ('LU', 'LC')
        AND link.linkprim IN ('P', 'C')
        AND m.date >= link.linkdt
        AND (m.date <= link.linkenddt OR link.linkenddt IS NULL)
    """
    return query

# Creates returns.csv file
SAVE_DIR = TABLES_DIR / "returns.csv"

# 1. Connect
db = wrds.Connection(wrds_username='username')

df_input = pd.read_csv(CIK_LIST)
ciks = df_input['CIK'].astype(str).str.zfill(10).tolist()

dfs = []

for cik in ciks:
    query = querymaker(cik)
    try:
        df = db.raw_sql(query)
        dfs.append(df)
        print(f"{cik}: ok ({len(df)} rows)")
    except Exception as e:
        print(f"{cik}: error: {e}")

df_all = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
df_all.to_csv(SAVE_DIR)