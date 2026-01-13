from pathlib import Path

# ------------------ Directories ------------------ 

ROOT_DIR = Path(__file__).resolve().parents[2]                             # Returns absolute path of risk_factor_pred folder

# ------------------ DATA Directories ------------------ 

DATA_DIR = ROOT_DIR / "data"
RAW_DIR = DATA_DIR / "raw"
INTERIM_DIR = DATA_DIR / "interim"
PROCESSED_DIR = DATA_DIR / "processed"

RAW_EDGAR_DIR = RAW_DIR / "sec-edgar-filings"
RAW_CIKS_DIR = RAW_DIR / "ciks_index"
INTERIM_CLEANED_DIR = INTERIM_DIR / "cleaned_filings"
INTERIM_ITEM1A_DIR = INTERIM_DIR / "item1a"
INTERIM_FEATURES_DIR = INTERIM_DIR / "text_features"
INTERIM_RETURNS_DIR = INTERIM_DIR / "returns"

PROCESSED_PANEL_PATH = PROCESSED_DIR / "panel.parquet"

# ------------------------------------------------------ 

OUTPUTS_DIR = ROOT_DIR / "outputs"
OUTPUTS_TABLES_DIR = OUTPUTS_DIR / "tables"
OUTPUTS_FIGURES_DIR = OUTPUTS_DIR / "figures"
OUTPUTS_LOGS_DIR = OUTPUTS_DIR / "logs"


TABLES_DIR = DATA_DIR / "tables"

CIK_LIST = RAW_CIKS_DIR / "cik_list.csv"                                     # Csv containing list of CIKS

SIMILARITY_DIR = INTERIM_FEATURES_DIR / "similarity.csv"

# ---------- SETTINGS ----------
FORM       = "10-K"                                                 # or "10-K", "10-KT", etc.
START_DATE = "2006-01-01"                                           # filings per CIK, only released after 2006
MAX_WORKERS = 4                                                     # number of threads
# -------------------------------

def ensure_project_dirs() -> None:
    for p in [
        RAW_EDGAR_DIR,
        RAW_CIKS_DIR,

        INTERIM_CLEANED_DIR,
        INTERIM_ITEM1A_DIR,
        INTERIM_FEATURES_DIR,
        INTERIM_RETURNS_DIR,

        PROCESSED_DIR,
        
        OUTPUTS_TABLES_DIR,
        OUTPUTS_FIGURES_DIR,
        OUTPUTS_LOGS_DIR,
    ]:
        p.mkdir(parents=True, exist_ok=True)


SIMILARITY_FIELDS = ["cik", "date_a", "date_b", "distance", "similarity", "len_a", "len_b", "sentiment"]