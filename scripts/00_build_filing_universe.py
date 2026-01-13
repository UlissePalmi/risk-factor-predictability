from risk_factor_pred.edgar import cik_index as cl
from risk_factor_pred.config import ensure_project_dirs

if __name__ == "__main__":

    # Setup data directories
    ensure_project_dirs()

    print("Starting cik_list.csv file generation... ")
    cl.cik_list_builder(start_year = 2006 , end_year = 2026)
