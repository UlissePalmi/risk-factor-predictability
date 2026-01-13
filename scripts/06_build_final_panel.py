from risk_factor_pred.config import SIMILARITY_FILE, FINAL_DATASET, RETURNS_FILE
from risk_factor_pred.datasets import build_panel as bp
import pandas as pd


if __name__ == "__main__":
      
    sim_df, return_df = bp.datatype_setup(pd.read_csv(SIMILARITY_FILE), pd.read_csv(RETURNS_FILE))

    sim_df = bp.merge_return(sim_df, return_df, months = 18, period = 'future')
    sim_df = bp.merge_return(sim_df, return_df, months = 12, period = 'past')
    
    print(sim_df.head())
    
    sim_df.to_csv(FINAL_DATASET, index=False)