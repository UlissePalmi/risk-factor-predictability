from risk_factor_pred.core import fx_splitter as fs, secDownloader as sd
from risk_factor_pred.consts import SEC_DIR

if __name__ == "__main__":

    # Create list of ciks from excel file or request cik in input
    ciks = [p.name for p in SEC_DIR.iterdir()] if sd.inputLetter() == 'l' else [input("Enter ticker...").upper()]

    # 
    fs.try_exercize(ciks)
