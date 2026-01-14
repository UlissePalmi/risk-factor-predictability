import pandas as pd
from pathlib import Path
from risk_factor_pred.config import TABLES_DIR

"""
This script cleans the features dataset by removing obvious bad comparisons.

It filters out rows where either filing is too short, converts `date_a` and
`date_b` to datetime, removes pairs that are too far apart in time (>= 500 days),
and saves the cleaned result as `cleanSimData.csv`.
"""

def remove_errors(df):
    """
    Remove obvious bad features rows and return a cleaned dataframe.

    Filters out very short filings, removes pairs that are too far apart in time,
    and returns the cleaned result for saving.
    """
    df = df[(df["len_a"] >= 75) & (df["len_b"] >= 75)]

    df["date_a"] = pd.to_datetime(df["date_a"],format="mixed",dayfirst=True)
    df["date_b"] = pd.to_datetime(df["date_b"],format="mixed",dayfirst=True)

    df['days'] = df['date_a'] - df['date_b']
    df['days'] = df['days'].dt.days.astype(int)

    df = df[df['days'] < 500]
    df = df.drop(columns='days')

data_folder = TABLES_DIR / "complete_features_data.xlsx"
SAVE_DIR = TABLES_DIR / "cleanSimData.csv"

df = remove_errors(pd.read_excel(data_folder))
df.to_csv(SAVE_DIR)