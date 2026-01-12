from typing import List, Dict, Tuple, Optional
from concurrent.futures import ProcessPoolExecutor
from risk_factor_pred.config import SEC_DIR, MAX_WORKERS
from itertools import islice
import re
import time

def _normalize_ws(s: str) -> str:
    """
    Replaces common non-breaking spaces with regular spaces, collapses runs of
    whitespace to a single space, and strips leading/trailing whitespace.
    """
    s = s.replace("\xa0", " ").replace("\u2007", " ").replace("\u202f", " ")
    s = re.sub(r'\s+', ' ', s).strip()
    return s

def before_dot(s: str) -> str:
    """
    Return everything before the first '.'; if no dot, return the string unchanged.
    """
    i = s.find('.')
    return s[:i] if i != -1 else s

def item_dict_builder(path):
    """
    Build an ordered list of detected 10-K 'Item' with their line number.

    Reads the file at 'path', scans line-by-line for headings that look like
    'Item 1.', 'Item 1A.', etc., and returns a list of dictionaries containing:
      - item_num: normalized item token (e.g., "1", "1A")
      - item_line: 1-indexed line number where the heading appears
        # Change to length of words/char post 'Item <num>.'
        # Must add 'Item <num> and <num>.'
    Consecutive duplicate item tokens are removed (deduped) to reduce noise.
    """

    text = path.read_text(encoding="utf-8", errors="ignore")
    out = []
    HEAD_RE = re.compile(r'^\s*(?P<kind>items?)\b\s*(?P<rest>[0-9].*)$', re.IGNORECASE)                                # Regex to find lines to split

    for i, raw in enumerate(text.splitlines(), start=1):
        line = _normalize_ws(raw)
        if not line:
            continue
        m = HEAD_RE.match(line)
        if not m or not m.group('rest'):
            continue
        label = m.group('rest')

        out.append({
            'item_num': before_dot(_normalize_ws(label).split()[0]).upper(),
            'item_line': i,
        })

    # dedupe consecutive duplicates
    deduped = []
    last = None
    for row in out:
        key = row['item_num'].lower()
        if key != last:
            deduped.append(row)
        last = key
    return deduped

def number_of_rounds(item_dict, bool):
    """
    Extract numeric item components and estimate how many full 'rounds' of items exist.

    For each entry in `item_dict:` list[dict], extracts only digits from the `item_num` field and
    converts them to integers. 
    Then estimates the number of repeated "rounds" of the table-of-contents items by
    counting occurrences of `max_num` and `max_num - 1`.
    """
    out = []
    for items in item_dict:
        digits = "".join(ch for ch in items.get("item_num") if ch.isdigit() and ch)
        out.append(digits)
    listAllItems = [int(i) for i in out]

    # sometimes "Item 400" exists
    while max(listAllItems) > 20:
        listAllItems.remove(max(listAllItems))
    
    max_num = max(listAllItems)

    # Double check the number of rounds is correct
    rounds = [i for i in listAllItems if i==max_num]
    rounds2 = [i for i in listAllItems if i==max_num-1]
    rounds = rounds2 if rounds > rounds2 else rounds
    
    if bool == True:
        return len(rounds)
    else:
        print(listAllItems)
        return listAllItems

def table_content_builder(filepath):
    """
    Builds a `tableContent` with a list of all the items in the 10K
    
    Returns: list[str]
    eg ['1', '1A', '1B', '1C', '2', ...]
    """
    item_dict = item_dict_builder(filepath)
    listAllItems = number_of_rounds(item_dict, bool=False)
    tableContent = ["1", "1A", "1B", "1C", "1D", "2", "3", "4", "5", "6", "7", "7A", "8"]
    letters_tuple = ("","A","B","C")
    for n in range(int(tableContent[-1])+1,max(listAllItems)+1):
        n = str(n)
        for l in letters_tuple:
            tableContent.append(n + l)    
    return tableContent

def item_segmentation_list(filepath):
    """
    Makes a list of dict that contains the actual items and where they should be segmented.
    
    Retreves the table of content of the 10-K with the table_content_builder function.
    Retreves the list of all the possible items and their location with the item_dict_builder function.
    
    First, builds multiple candidate sequences of item headings by scanning in item_dict.
    Secondly, Selects the candidate that is most probably the item list

    Returns list[dict]: The selected sequence (list of dicts with 'Item number' and 'Item line').
    """
    tableContent = table_content_builder(filepath)
    item_dict = item_dict_builder(filepath)

    list_lines = []
    last_ele = 0
    for _ in range(number_of_rounds(item_dict, bool=True)):
        lines = []
        for itemTC in tableContent:
            for r in item_dict:
                if itemTC == r.get('item_num') and r.get('item_line') > last_ele:
                    lines.append(r)
                    last_ele = r['item_line']
                    break
        list_lines.append(lines)

    # ----- Choose candidate with greatest character span -----

    if len(list_lines) == 1:
        print(list_lines[0])
        return list_lines[0]

    with open(filepath, "r", encoding="utf-8", errors="replace") as f:
        text = f.read()

    def _line_start_offsets(text: str):
        starts = [0]
        for line in text.splitlines(keepends=True):
            starts.append(starts[-1] + len(line))
        return starts

    def _normalize_line_index(item_line: int, num_lines: int) -> int:
        if item_line is None:
            return 0
        if 1 <= item_line <= num_lines:  # likely 1-based
            return item_line - 1
        return max(0, min(item_line, num_lines - 1))

    line_start_char = _line_start_offsets(text)
    num_lines = len(text.splitlines())

    best_i = 0
    best_span = float("-inf")

    for i, cand in enumerate(list_lines):
        start_line = _normalize_line_index(cand[1]["item_line"], num_lines)
        end_line = _normalize_line_index(cand[-1]["item_line"], num_lines)

        span_chars = line_start_char[end_line] - line_start_char[start_line]

        if span_chars > best_span:
            best_span = span_chars
            best_i = i
    
    print(list_lines[best_i])
    return list_lines[best_i]

def print_items(cik):
    """
    Write per-item text files by slicing the input document between detected item headings.

    For each entry in `final_split`, this function:
      - takes the line range from its `line_no` to the next item heading's `line_no`,
      - writes the extracted chunk to `p/item<ITEM>.txt` (e.g., item1A.txt).

    Parameters
    ----------
    filepath : pathlib.Path
        Path to the full cleaned filing text (e.g., clean-full-submission.txt).
    final_split : list[dict]
        Selected sequence of headings (output of `final_list`), containing 'item_n' and 'line_no'.
    p : pathlib.Path
        Output directory where item files will be written (typically the filing folder).
    """
    try:
        path = SEC_DIR / cik / '10-K'
        for filing in path.iterdir():
            p = path / filing
            filepath = p / "full-submission.txt"
            item_segmentation = item_segmentation_list(filepath)
            page_list = [i['item_line'] for i in item_segmentation]
            page_list.append(11849)

            for n, i in enumerate(item_segmentation):
                start, end = page_list[n], page_list[n+1]
                with filepath.open("r", encoding="utf-8", errors="replace") as f:
                    lines = list(islice(f, start - 1, end-1))
                chunk = "".join(lines)
                filename = f"item{i['item_num']}.txt"

                full_path = p / filename
                with open(full_path, "w", encoding='utf-8') as f:
                    f.write(chunk)
            print("okkkkk")
            time.sleep(0.5)
    except:
        print("failed")
    return

def try_exercize(ciks: list):
    """
        Runs print_items in parallel
    """
    with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
        list(executor.map(print_items, ciks))
    return