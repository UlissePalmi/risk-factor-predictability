from risk_factor_pred.core import similarity as sm
from risk_factor_pred.config import SIMILARITY_FIELDS, SIMILARITY_DIR, INTERIM_CLEANED_DIR
import csv

from risk_factor_pred.edgar import cik_index as cl


if __name__ == "__main__":

    # Create list of ciks from excel file or request cik in input
    ciks = [p.name for p in INTERIM_CLEANED_DIR.iterdir()] if cl.inputLetter() == 'l' else [input("Enter cik...").upper()]
    
    with open(SIMILARITY_DIR, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=SIMILARITY_FIELDS)
        writer.writeheader()
        
        sm.concurrency_runner(writer, ciks)

    