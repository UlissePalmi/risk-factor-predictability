from concurrent.futures import ProcessPoolExecutor, as_completed
from nltk.sentiment import SentimentIntensityAnalyzer
from risk_factor_pred.config import INTERIM_ITEM1A_DIR, MAX_WORKERS, INTERIM_CLEANED_DIR
import nltk
import sys
import re

nltk.download("vader_lexicon", quiet=True)
_sia = SentimentIntensityAnalyzer()

# --------------------------------------------------------------------------------------------------------------------
#                                                MAKE COMPS FUNCTIONS
# --------------------------------------------------------------------------------------------------------------------

def check_date(folder):
    """
    Read a filing folder and extract the submission date for that accession.
    Returns a dict with "year", "month", "day", and "filing".
    """
    filing = folder.name
    file = folder / "full-submission.txt"
    with open(file, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            hay = line.lower()
            if filing in hay:
                date = hay.partition(":")[2].lstrip()
                break
    info_dict = {
        "year": date[:4],
        "month": date[4:6],
        "day": date[6:8],
        "filing": filing
    }
    return info_dict

def order_filings(records):
    """
    Sort filing records by release date (newest to oldest).
    Returns a list of [filing_id, filing_date] pairs.
    """
    records_sorted = sorted(
        records,
        key=lambda r: (int(r["year"]), int(r["month"]), int(r["day"])),
        reverse=True,
    )

    out = []
    for r in records_sorted:
        filing_id = r["filing"]
        filing_date = f"{int(r['year']):04d}-{int(r['month']):02d}-{int(r['day']):02d}"
        out.append([filing_id, filing_date])
    return out

def make_comps(cik):
    """
    Build consecutive Item 1A comparison pairs for a single CIK.

    Uses available `item1A.txt` filings, orders them by date, and returns
    a list of {date1, filing1, date2, filing2} dicts.
    """
    print(cik)
    date_data = []
    folders_path = INTERIM_ITEM1A_DIR / cik / "10-K"
    checkdate_path = INTERIM_CLEANED_DIR / cik / "10-K"
    
    for i in folders_path.iterdir():
        if not (i / "item1A.txt").is_file():
            continue

        date_data.append(check_date(checkdate_path / i.name) if (i / "item1A.txt").is_file() else None) 
    ordered_filings = order_filings(date_data)

    comps_list = []
    for n in range(1, len(ordered_filings)):
        comps_list.append({
            "date1": ordered_filings[n - 1][1],
            "filing1": ordered_filings[n - 1][0],
            "date2": ordered_filings[n][1],
            "filing2": ordered_filings[n][0]
        })
    return comps_list

def concurrency_runner(writer, ciks):
    """
    Compute Levenshtein edit distance features for multiple CIKs using multiprocessing.
    Runs `worker()` per CIK and writes the resulting rows to the output CSV.
    """
    try:
        with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(worker, cik): cik for cik in ciks}
            
            for fut in as_completed(futures):
                print(fut)
                rows = fut.result()
                writer.writerows(rows)
    except:
        print("Skipped")

# ---------------------------------------------------------------------------------------

def worker(cik):
    """
    Compute feature rows for all consecutive filing comparisons for a single CIK.
    Returns a list of row dictionaries for writing to the output file.
    """
    comps = make_comps(cik)
    rows = []
    [rows.append(process_comps(comp, cik)) for comp in comps]
    print(rows)
    return rows


def process_comps(comp, cik):
    """
    Load two Item 1A texts for a comparison pair and compute feature metrics.
    Returns the output dictionary produced by `min_edit_levenshtein()`.
    """
    filingNew, filingOld = comp["filing1"], comp["filing2"]
    fileNew = INTERIM_ITEM1A_DIR / cik / "10-K" / filingNew / "item1A.txt"
    fileOld = INTERIM_ITEM1A_DIR / cik / "10-K" / filingOld / "item1A.txt"
    textNew = fileNew.read_text(encoding="utf-8", errors="ignore")
    textOld = fileOld.read_text(encoding="utf-8", errors="ignore")
    return min_edit_levenshtein(textNew, textOld, comp, cik)

# --------------------------------------------------------------------------------------------------------------------
#                                                VARIABLES FUNCTIONS
# --------------------------------------------------------------------------------------------------------------------


def tokenize(text: str) -> list[str]:
    """
    Returns list of all elements in the string in lowercase.
    """
    _WORD_RE = re.compile(r"[A-Za-z']+")
    return _WORD_RE.findall(text.lower())

def mean_vader_compound(words) -> float:
    """
    Compute the average VADER compound score over a list of words.
    Returns 0.0 if the input list is empty.
    """
    compounds = []
    for w in words:
        w = (w or "").strip()
        scores = {"compound": 0.0} if not w else _sia.polarity_scores(w)
        compounds.append(scores["compound"])
    return sum(compounds) / len(compounds) if len(compounds) != 0 else 0

def levenshtein_tokens(a_tokens, b_tokens, cik):
    """
    Compute token-level Levenshtein distance and identify newly introduced tokens.
    Returns (distance, new_words).
    """
    m, n = len(a_tokens), len(b_tokens)
    if n > m:
        # ensure n <= m for memory efficiency
        a_tokens, b_tokens = b_tokens, a_tokens
        m, n = n, m

    prev = list(range(n + 1))  # row 0..n
    for i in range(1, m + 1):
        cur = [i] + [0]*n
        ai = a_tokens[i-1]
        for j in range(1, n + 1):
            cost = 0 if ai == b_tokens[j-1] else 1
            cur[j] = min(
                prev[j] + 1,      # deletion
                cur[j-1] + 1,     # insertion
                prev[j-1] + cost  # substitution (0 if match)
            )

        if (j % 1000 == 0) or (j == n):  # adjust 1000 for your speed/verbosity
            done_cells = (i - 1) * n + j
            total_cells = m * n
            pct = (done_cells / total_cells) * 100 if total_cells else 100.0
            sys.stdout.write(f"\rCik: {cik} Progress: {pct:6.2f}%  (row {i}/{m}, col {j}/{n})")
            sys.stdout.flush()

        b_set = set(b_tokens)
        new_words = [t for t in a_tokens if t not in b_set]
    # after both loops finish:
        print()
        prev = cur
    return prev[n], new_words

def jaccard_similarity(text_a: str, text_b: str) -> float:
    """
    Compute Jaccard similarity between the token sets of two texts.
    """
    A = set(tokenize(text_a))
    B = set(tokenize(text_b))
    if not A and not B:
        return 1.0
    return len(A & B) / len(A | B)

def min_edit_levenshtein(text_a: str, text_b: str, dict, cik):
    """
    Compute disclosure-change features between two Item 1A texts.
    Returns a dictionary with levenshtein, lengths, and sentiment of newly added words.
    """
    A, B = tokenize(text_a), tokenize(text_b)
    dist, new_words = levenshtein_tokens(A, B, cik)
    denom = len(A) + len(B)
    lev = 1.0 - (dist / denom if denom else 0.0)
    return {
        "cik": cik, 
        "date_a": dict["date1"], 
        "date_b": dict["date2"], 
        "distance": dist, 
        "levenshtein": lev, 
        "len_a": len(A), 
        "len_b": len(B), 
        "sentiment": mean_vader_compound(new_words)
        }
