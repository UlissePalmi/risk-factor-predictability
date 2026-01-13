from risk_factor_pred.config import SIMILARITY_FILE, FINAL_DATASET, RETURNS_FILE
import pandas as pd

def datatype_setup(sim_df, return_df):
    sim_df["date_a"] = pd.to_datetime(sim_df["date_a"],format="mixed",dayfirst=True)
    sim_df["date_b"] = pd.to_datetime(sim_df["date_b"],format="mixed",dayfirst=True)
    return_df["date"] = pd.to_datetime(return_df["date"])

    return_df['retPlusOne'] = return_df['ret'] + 1
    return sim_df, return_df

def merge_return(sim_df, return_df, months, period):
    if period == 'future':
        sim_df["start_anchor"] = (sim_df["date_a"] + pd.offsets.MonthBegin(1)).dt.normalize()
        sim_df["end_anchor"]   = sim_df["start_anchor"] + pd.offsets.DateOffset(months=months)
    elif period == 'past':
        sim_df["end_anchor"]   = sim_df["date_a"]
        sim_df["start_anchor"] = sim_df["end_anchor"] - pd.DateOffset(months=months)
    
    sim_df = sim_df.reset_index().rename(columns={'index': 'sim_idx'}) 

    # CIKs are turned into strings
    sim_df["cik"] = sim_df["cik"].astype(str).str.zfill(10)
    return_df = return_df.copy()
    return_df["cik"] = (return_df["cik"].astype(str).str.replace(r"\.0$", "", regex=True).str.zfill(10))

    # 4. Merge sim_df with returns on firm identifier
    merged = sim_df.merge(
        return_df[["cik", "date", "retPlusOne"]],
        on="cik",
        how="left"
    )

    # 5. Filter to dates in [prev_start, start_anchor)
    merged = merged[
        (merged['date'] >= merged['start_anchor']) &
        (merged['date'] <= merged['end_anchor'])     # use < if you want end exclusive
    ].sort_values(["sim_idx", "date"])

    # 6. Compute return over that window for each original row
    prod_window = merged.groupby("sim_idx")["retPlusOne"].prod()

    # 7. Map back to sim_df as a new column
    sim_df[f"{period}_{months}m_ret"] = (sim_df["sim_idx"].map(prod_window) - 1) * 100

    return sim_df.drop(columns=['start_anchor','end_anchor','sim_idx'])
