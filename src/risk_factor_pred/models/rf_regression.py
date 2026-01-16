from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.ensemble import RandomForestRegressor
import numpy as np

def rf_reg(X, y, df):
    """
    Train and evaluate a Random Forest regressor to predict future returns.

    Fits the model on a train/test split, prints MAE/RMSE/R², and writes full-sample
    predictions to `pred_check.csv` for inspection.
    """
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.20, random_state=42
    )

    # Random Forest model for returns
    rf = RandomForestRegressor(
        n_estimators=300,       # number of trees
        criterion="squared_error",
        max_depth=4,            # limit depth to reduce overfitting
        min_samples_leaf=200,   # similar robustness to your tree
        n_jobs=-1,              # use all cores
        random_state=42,
        oob_score=True          # out-of-bag score as extra validation
    )

    rf.fit(X_train, y_train)
    pred = rf.predict(X_test)

    mae  = mean_absolute_error(y_test, pred)
    rmse = np.sqrt(mean_squared_error(y_test, pred))
    r2   = r2_score(y_test, pred)

    print(f"Random Forest | Test MAE={mae:.4f} | RMSE={rmse:.4f} | R²={r2:.3f}")
    print(f"OOB R² (if applicable) = {rf.oob_score_:.3f}")

    y_pred = rf.predict(X)

    df['y_pred'] = y_pred
    df.to_csv('pred_check.csv')
