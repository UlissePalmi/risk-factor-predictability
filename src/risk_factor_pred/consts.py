from pathlib import Path

# ------------------ Directories ------------------ 

REPO_ROOT = Path(__file__).resolve().parents[2]                             # Returns absolute path of risk_factor_pred folder
DATA_DIR = REPO_ROOT / "data"

HTML_DIR = DATA_DIR / "html"
HTML_DIR.mkdir(parents=True, exist_ok=True)
SEC_DIR = HTML_DIR / "sec-edgar-filings"

TABLES_DIR = DATA_DIR / "tables"
CIK_LIST = TABLES_DIR / "cik_list.xlsx"                                     # Excel containing list of CIKS

# ---------- SETTINGS ----------
FORM       = "10-K"                                                 # or "10-K", "10-KT", etc.
START_DATE = "2006-01-01"                                           # filings per CIK, only released after 2006
MAX_WORKERS = 4                                                     # number of threads
# -------------------------------
