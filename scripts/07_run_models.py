from risk_factor_pred.models import rf_setup as rs, rf_classification as rc, rf_regression as rr
from risk_factor_pred.config import FINAL_DATASET
import pandas as pd

df = pd.read_csv(FINAL_DATASET)

df = rs.feature_engineering(df)

# Features X and classification target y
df_cat, labels = rc.create_labels(df, prediction_col = "future_18m_ret")
X, y = rs.X_y_builder(df_cat)
rc.rf_cat(X, y, labels)

df["prediction"] = df["future_18m_ret"]
X, y = rs.X_y_builder(df)
rr.rf_reg(X, y, df)