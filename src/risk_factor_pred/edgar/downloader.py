from concurrent.futures import ThreadPoolExecutor, as_completed
from risk_factor_pred.config import FORM, START_DATE, MAX_WORKERS, RAW_DIR
import time
from sec_edgar_downloader import Downloader

def download_for_cik(cik: str):
    """
    Download SEC filings for a given CIK using `sec-edgar-downloader`.
    """
    time.sleep(0.1)
    dl = Downloader("MyCompanyName", "my.email@domain.com", str(RAW_DIR))
    print(f"Starting {FORM} for CIK {cik}")
    try:
        dl.get(FORM, cik, after=START_DATE)
        time.sleep(10)
        return cik, "ok", None
    except ValueError as e:
        return cik, "not_found", str(e)
    except Exception as e:
        return cik, "error", str(e)

def download(ciks):
    """
    Download and clean filings for a collection of CIKs using multithreading.

    The function submits one task per CIK to a ThreadPoolExecutor and consumes
    results as tasks finish using `as_completed`. It prints a progress counter
    and summarizes not-found CIKs and errors at the end.
    """
    total = len(ciks)
    print(f"Found {total} unique CIKs")

    not_found = []
    errors = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(download_for_cik, cik): cik for cik in ciks}

        # as_completed lets them run in parallel; we consume results as they finish
        for idx, future in enumerate(as_completed(futures), start=1):
            cik, status, err = future.result()
            print(f"[{idx}/{total}] CIK {cik}: {status}")
            if status == "not_found":
                not_found.append(cik)
            elif status == "error":
                errors.append((cik, err))

    if not_found:
        print("\nCIKs not found:")
        for cik in not_found:
            print(" ", cik)

    if errors:

        print("\nCIKs with errors:")
        for cik, err in errors:
            print(f" {cik}: {err}")
    return




