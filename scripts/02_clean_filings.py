from risk_factor_pred.core import html_cleaner as hc
from risk_factor_pred.config import RAW_EDGAR_DIR
from risk_factor_pred.edgar import cik_index as cl

if __name__ == "__main__":
    
    # Create list of ciks from excel file or request cik in input
    ciks = [cik.name for cik in RAW_EDGAR_DIR.iterdir()] if cl.inputLetter() == 'l' else [input("Enter CIK...").zfill(10)]
    
    # Remove HTML tags from each element of previously created list
    [hc.cleaner((cik), output_filename = "full-submission.txt") for cik in ciks]