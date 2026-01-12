import pandas as pd
from pathlib import Path
from risk_factor_pred.config import TABLES_DIR

# Creates cleanSimData.csv file

def remove_errors(df):
    df = df[(df["len_a"] >= 75) & (df["len_b"] >= 75)]

    df["date_a"] = pd.to_datetime(df["date_a"],format="mixed",dayfirst=True)
    df["date_b"] = pd.to_datetime(df["date_b"],format="mixed",dayfirst=True)

    df['days'] = df['date_a'] - df['date_b']
    df['days'] = df['days'].dt.days.astype(int)

    df = df[df['days'] < 500]
    df = df.drop(columns='days')

data_folder = TABLES_DIR / "complete_similarity_data.xlsx"
SAVE_DIR = TABLES_DIR / "cleanSimData.csv"

df = remove_errors(pd.read_excel(data_folder))
df.to_csv(SAVE_DIR)