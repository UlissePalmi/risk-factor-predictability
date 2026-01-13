from risk_factor_pred.config import RETURNS_FILE
from risk_factor_pred.wrds import crsp_returns as cr



if __name__ == "__main__":

    return_df = cr.df_with_returns()
    return_df.to_csv(RETURNS_FILE, index=False)
