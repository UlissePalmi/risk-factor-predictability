import pandas as pd
from pathlib import Path

# Creates cleanSimData.csv file

data_folder = Path("data") / "tables" / "complete_similarity_data.xlsx"
SAVE_DIR = Path("data") / "tables" / "cleanSimData.csv"

df = pd.read_excel(data_folder)
df = df[(df["len_a"] >= 75) & (df["len_b"] >= 75)]
print(df)
df["date_a"] = pd.to_datetime(df["date_a"],format="mixed",dayfirst=True)
df["date_b"] = pd.to_datetime(df["date_b"],format="mixed",dayfirst=True)

df['days'] = df['date_a'] - df['date_b']
df['days'] = df['days'].dt.days.astype(int)
df = df[df['days'] < 500]
df = df.drop(columns='days')
#print(days)
print(df)

df.to_csv(SAVE_DIR)