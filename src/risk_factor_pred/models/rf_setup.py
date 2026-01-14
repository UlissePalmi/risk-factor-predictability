import pandas as pd

def feature_engineering(df):
    for col in ["date_a", "date_b"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    # sentiment sign: 1 if >= 0 else 0 (you can flip to {-1,1} if you prefer)
    df["sentiment_pos"] = (df["sentiment"] >= 0).astype(int)
    df["sim_below_70"] = df["similarity"] < 0.70
    df["len_growth_pct"] = df['len_a'] / df['len_b'] - 1
    df["inc_len"] = df["len_a"] > df["len_b"]
    return df

def X_y_builder(df):
    feature_cols = [
        "similarity",
        "sentiment",
        "sentiment_pos",
        "sim_below_70"
        "inc_len",
        "len_growth_pct",
        "old_similarity",
        "past_12m_ret",
    ]
    X = df[feature_cols]
    y = df["prediction"]

    mask = X.notna().all(axis=1) & y.notna()
    X = X[mask]
    y = y[mask]
    return X, y
