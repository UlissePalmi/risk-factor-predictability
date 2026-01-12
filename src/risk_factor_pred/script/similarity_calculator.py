from risk_factor_pred.core import fx_similarity as sf, secDownloader as sd
from risk_factor_pred.consts import TABLES_DIR, SEC_DIR
import csv

SAVE_DIR = TABLES_DIR / "similarity.csv"

if __name__ == "__main__":

    # Create list of ciks from excel file or request cik in input
    ciks = [p.name for p in SEC_DIR.iterdir()] if sd.inputLetter() == 'l' else [input("Enter cik...").upper()]
    
    fieldnames = ["cik", "date_a", "date_b", "distance", "similarity", "len_a", "len_b", "sentiment"]
    with open(SAVE_DIR, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        
        sf.concurrency_runner(writer, ciks)

    