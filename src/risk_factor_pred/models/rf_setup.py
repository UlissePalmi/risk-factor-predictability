import pandas as pd

def feature_engineering(df):
    """
    Create model features from levenshtein, sentiment, and length-based inputs.

    Converts date columns to datetime and adds derived features used by the
    regression and classification models.
    """
    for col in ["date_a", "date_b"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    # sentiment sign: 1 if >= 0 else 0 (you can flip to {-1,1} if you prefer)
    df["old_levenshtein"] = df["levenshtein"].shift(1)
    df["sentiment_pos"] = (df["sentiment"] >= 0).astype(int)
    df["lev_below_70"] = df["levenshtein"] < 0.70
    df["len_growth_pct"] = df['len_a'] / df['len_b'] - 1
    df["inc_len"] = df["len_a"] > df["len_b"]
    df = df.dropna()

    return df

def X_y_builder(df):
    """
    Build the feature matrix `X` and target vector `y` for modeling.

    Selects the feature columns, extracts `prediction` as the target,
    and drops rows with missing values.
    """
    feature_cols = [
        "levenshtein",
        "sentiment",
        "sentiment_pos",
        "lev_below_70",
        "inc_len",
        "len_growth_pct",
        "old_levenshtein",
        "past_12m_ret",
    ]
    X = df[feature_cols]
    y = df["prediction"]

    mask = X.notna().all(axis=1) & y.notna()
    X = X[mask]
    y = y[mask]
    return X, y
