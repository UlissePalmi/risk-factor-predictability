from risk_factor_pred.core import cik_list_builder as cl, html_cleaner as hc
from risk_factor_pred.config import SEC_DIR

if __name__ == "__main__":
    
    # Create list of ciks from excel file or request cik in input
    ciks = [cik.name for cik in SEC_DIR.iterdir()] if cl.inputLetter() == 'l' else [input("Enter CIK...").zfill(10)]
    
    # Remove HTML tags from each element of previously created list
    [hc.cleaner((cik), output_filename = "full-submission.txt") for cik in ciks]