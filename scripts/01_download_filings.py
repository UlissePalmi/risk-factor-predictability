from risk_factor_pred.edgar import downloader as sd
from risk_factor_pred.edgar import cik_index as cl

if __name__ == "__main__":
    
    # Create list of ciks from excel file or request cik in input
    ciks = cl.load_unique_ciks() if cl.inputLetter() == 'l' else [input("Enter CIK...").upper()]
    
    # Download 10-K and Remove HTML tags from previously created list
    sd.download_n_clean(ciks)