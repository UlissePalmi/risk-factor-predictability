import pandas as pd
from pathlib import Path

# Creates merged_dataset.csv file

sim_DIR = Path("data") / "tables" / "cleanSimData.csv"
returns_DIR = Path("data") / "tables" / "returns.csv"
SAVE_DIR = Path("data") / "tables" / "merged_dataset.csv"

sim_df = pd.read_csv(sim_DIR)
return_df = pd.read_csv(returns_DIR)
      
sim_df["date_a"] = pd.to_datetime(sim_df["date_a"],format="mixed",dayfirst=True)
sim_df["date_b"] = pd.to_datetime(sim_df["date_b"],format="mixed",dayfirst=True)
return_df["date"] = pd.to_datetime(return_df["date"])

sim_df["start_anchor"] = (sim_df["date_a"] + pd.offsets.MonthBegin(1)).dt.normalize()
sim_df['prev_start'] = sim_df['start_anchor'] - pd.DateOffset(years=1)
sim_df["end_anchor"]   = sim_df["start_anchor"] + pd.offsets.DateOffset(months=18)

return_df['retPlusOne'] = return_df['ret'] + 1

#print(sim_df)
#print(return_df)

sim_df['ret_18'] = sim_df['ticker'].map(
    return_df
    .groupby('cik')['retPlusOne']
    .mean()
)

sim_tmp = sim_df.reset_index().rename(columns={'index': 'sim_idx'})

merged = sim_tmp.merge(
    return_df[['cik', 'date', 'retPlusOne']],
    left_on='ticker',    # ticker in sim_df
    right_on='cik',      # cik in return_df
    how='left'
)
merged = merged[
    (merged['date'] >= merged['start_anchor']) &
    (merged['date'] <= merged['end_anchor'])     # use < if you want end exclusive
]

mean_window = merged.groupby('sim_idx')['retPlusOne'].mean()
sim_df['ret_18'] = sim_df.index.to_series().map(mean_window)
sim_df['ret_18'] = (sim_df['ret_18'] - 1)*100
sim_df = sim_df.dropna(subset=['ret_18'])

#find a way to account for bankrupcy/merger


sim_tmp = sim_df.reset_index().rename(columns={'index': 'sim_idx'})

# 4. Merge sim_df with returns on firm identifier
merged_prev = sim_tmp.merge(
    return_df[['cik', 'date', 'retPlusOne']],
    left_on='ticker',        # firm id in sim_df
    right_on='cik',          # firm id in return_df
    how='left'
)

# 5. Filter to dates in [prev_start, start_anchor)
merged_prev = merged_prev[
    (merged_prev['date'] >= merged_prev['prev_start']) &
    (merged_prev['date'] <  merged_prev['start_anchor'])
]

# 6. Compute mean return over that window for each original row
mean_prev_12m = merged_prev.groupby('sim_idx')['retPlusOne'].mean()

# 7. Map back to sim_df as a new column
sim_df['ret_prev_12m'] = sim_df.index.to_series().map(mean_prev_12m)
sim_df['ret_prev_12m'] = (sim_df['ret_prev_12m'] - 1)*100
# Optional: drop helper column
sim_df.drop(columns=['prev_start'], inplace=True)


sim_df['date_a'] = pd.to_datetime(sim_df['date_a'])
sim_df = sim_df.sort_values(['ticker', 'date_a'])

# Add old_similarity = previous year's similarity for same cik
sim_df['old_similarity'] = sim_df.groupby('ticker')['similarity'].shift(1)

sim_df.to_csv(SAVE_DIR)
