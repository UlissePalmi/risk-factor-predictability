from risk_factor_pred.config import INTERIM_CLEANED_DIR
from risk_factor_pred.core import item_splitter as si
from risk_factor_pred.edgar import cik_index as cl


if __name__ == "__main__":

    # Create list of ciks from excel file or request cik in input
    ciks = [p.name for p in INTERIM_CLEANED_DIR.iterdir()] if cl.inputLetter() == 'l' else [input("Enter ticker...").upper()]

    # 
    si.try_exercize(ciks)
