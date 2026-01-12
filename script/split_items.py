from risk_factor_pred.core import cik_list_builder as cl, item_splitter as si
from risk_factor_pred.config import SEC_DIR

if __name__ == "__main__":

    # Create list of ciks from excel file or request cik in input
    ciks = [p.name for p in SEC_DIR.iterdir()] if cl.inputLetter() == 'l' else [input("Enter ticker...").upper()]

    # 
    si.try_exercize(ciks)
