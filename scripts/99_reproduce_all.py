from risk_factor_pred.text import tokenize as sm
from risk_factor_pred.config import CIK_LIST, SIMILARITY_FIELDS, SIMILARITY_DIR
import csv

from risk_factor_pred.edgar import cik_index as cl, downloader as sd
from risk_factor_pred.text import segment as si

# folder.file inside files with functions
if __name__ == "__main__":
    ciks = cl.load_unique_ciks(CIK_LIST)

    # Open file config to change number of cores used
    sd.download_n_clean(ciks)

    # Split items
    si.try_exercize(ciks)

    # Similarity
    with open(SIMILARITY_DIR, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=SIMILARITY_FIELDS)
        writer.writeheader()
        
        sm.concurrency_runner(writer, ciks)
