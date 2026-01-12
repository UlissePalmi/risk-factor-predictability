from risk_factor_pred.core import cik_list_builder as cl, similarity as sm
from risk_factor_pred.config import TABLES_DIR, SEC_DIR, SIMILARITY_FIELDS, SIMILARITY_DIR
import csv


if __name__ == "__main__":

    # Create list of ciks from excel file or request cik in input
    ciks = [p.name for p in SEC_DIR.iterdir()] if cl.inputLetter() == 'l' else [input("Enter cik...").upper()]
    
    with open(SIMILARITY_DIR, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=SIMILARITY_FIELDS)
        writer.writeheader()
        
        sm.concurrency_runner(writer, ciks)

    