from risk_factor_pred.consts import SEC_DIR
import re
from typing import List
import os

# --------------------------------------------------------------------------------------------------------------------
#                                              REGEX FOR HTML CLEANING
# --------------------------------------------------------------------------------------------------------------------

def remove_xbrl_xml_blocks(html_content):
    """
    This function deletes:
      - entire <XBRL>...</XBRL> or <XML>...</XML> blocks,
      - individual inline XBRL tags like <ix:...> or </ix:...>.
    """
    pattern_blocks = re.compile(r'(<XBRL.*?>.*?</XBRL>)|(<XML.*?>.*?</XML>)', re.DOTALL)
    pattern_tags = re.compile(r'</?ix:.*?>')
    clean_content = re.sub(pattern_blocks, '', html_content)
    clean_content = re.sub(pattern_tags, '', clean_content)
    return clean_content

def _ends_with_tag(piece: str) -> bool:
    """
    Heuristically determine whether a text fragment ends with an HTML tag.

    The check:
      - trims trailing whitespace,
      - confirms the string ends with '>',
      - searches the last up to 300 characters for the most recent '<',
      - ensures there is no newline between that '<' and the end.

    Returns True if the fragment likely ends with an HTML tag, otherwise False.
    """
    s = piece.rstrip()
    if not s.endswith(">"):
        return False
    tail = s[-300:] if len(s) > 300 else s
    i = tail.rfind("<")
    return i != -1 and "\n" not in tail[i:]

def _starts_with_tag(line: str) -> bool:
    """
    Returns True if the first non-whitespace character is '<', else False.
    """
    return line.lstrip().startswith("<")

def soft_unwrap_html_lines(html: str) -> str: # Removes /n if sentence is ongoing
    """
    Join lines that appear to be mid-sentence.

    Lines are joined when:
      - the current logical line does NOT end with an HTML tag, AND
      - the next line does NOT start with an HTML tag.

    When joining, the function enforces exactly one space at the boundary.
    """
    lines = html.splitlines()
    if not lines:
        return html

    out_lines = []

    parts = [lines[0].rstrip("\r")]
    cur_ends_with_tag = _ends_with_tag(parts[-1])

    for raw_next in lines[1:]:
        nxt = raw_next.rstrip("\r")
        next_starts_tag = _starts_with_tag(nxt)

        if (not cur_ends_with_tag) and (not next_starts_tag):
            # join: ensure exactly one space at the boundary
            if parts[-1] and parts[-1].endswith((" ", "\t")):
                parts[-1] = parts[-1].rstrip()
            parts.append(" ")
            parts.append(nxt.lstrip())
            # after joining, the 'current line' ends as nxt ends
            cur_ends_with_tag = _ends_with_tag(nxt)
        else:
            # flush current logical line
            out_lines.append("".join(parts))
            # start a new logical line
            parts = [nxt]
            cur_ends_with_tag = _ends_with_tag(nxt)

    # flush the last line
    out_lines.append("".join(parts))
    return "\n".join(out_lines)

def remove_head_with_regex(html_content):
    """
    Remove the <head>...</head> section from HTML content.
    """
    pattern = re.compile(r'<head>.*?</head>', re.DOTALL | re.IGNORECASE)
    clean_content = re.sub(pattern, '', html_content)
    return clean_content

def remove_style_with_regex(html_content):
    """
    Strip inline 'style' attributes from all HTML tags.
    """
    pattern = re.compile(r'\sstyle=(["\']).*?\1', re.IGNORECASE)
    clean_content = re.sub(pattern, '', html_content)
    return clean_content

def remove_id_with_regex(html_content):
    """
    Strip 'id' attributes from all HTML tags.
    """
    pattern = re.compile(r'\s+id=(["\']).*?\1', re.IGNORECASE)
    clean_content = re.sub(pattern, '', html_content)
    return clean_content

def remove_align_with_regex(html_content):
    """
    Strip 'align' attributes from all HTML tags.
    """
    pattern = re.compile(r'\s+align=(["\']).*?\1', re.IGNORECASE)
    clean_content = re.sub(pattern, '', html_content)
    return clean_content

def remove_part_1(html_content): # Cleans comments, tables, img, span
    """
    Operations performed:
      - remove HTML comments <!-- ... -->,
      - remove <img ...> tags,
      - replace certain HTML entities with ASCII equivalents,
      - remove numeric character references of the form '&#ddd;'.
    """
    pattern = re.compile(r'<!--.*?-->', re.DOTALL)
    html_content = re.sub(pattern, '', html_content)
    
    pattern = re.compile(r'<img.*?>', re.IGNORECASE)
    html_content = re.sub(pattern, '', html_content)

    html_content = html_content.replace('<span>', '').replace('</span>', '').replace('&#8217;', "'").replace('&#8220;', '"').replace('&#8221;', '"')
    html_content = html_content.replace('&nbsp;', ' ').replace('&#146;', "'")

    pattern = re.compile(r'&#\d{3};')
    html_content = re.sub(pattern, ' ', html_content)

    return html_content

def loop_clean(html_content):
    """
    Iteratively remove empty <p>...</p> and <div>...</div> tags until stable.
    """
    p_pattern = re.compile(r'<p>\s*</p>', re.DOTALL | re.IGNORECASE)
    div_pattern = re.compile(r'<div>\s*</div>', re.IGNORECASE)
    while True:
        pre_cleaning_content = html_content
        
        html_content = re.sub(p_pattern, '', html_content)
        html_content = re.sub(div_pattern, '', html_content)

        if html_content == pre_cleaning_content:
            break

    return html_content

def remove_numeric_entities(s: str) -> str:
    """
    Remove numeric HTML entities such as '&#123;' or '&#x1F4A9;'.
    """
    return re.sub(r'&#(?:\d{1,8}|[xX][0-9A-Fa-f]{1,8});', '', s)

def unwrap_tags(html_content): # Removes matching <ix...> and </ix...> tags but keeps the content between them.
    """
    This function removes/replaces a set of tags commonly found in SEC filings,
    inserting newlines for structural tags and deleting closing tags. It also
    removes table-related tags (<table>, <tr>, <td>) by converting some to newlines.
    """
    pattern = re.compile(r'<ix:[a-zA-Z0-9_:]+.*?>', re.IGNORECASE)
    html_content = re.sub(pattern, '\n', html_content)

    pattern = re.compile(r'</ix:[a-zA-Z0-9_:]+>', re.IGNORECASE)
    html_content = re.sub(pattern, '', html_content)

    pattern = re.compile(r'<html.*?>', re.IGNORECASE | re.DOTALL)
    html_content = re.sub(pattern, '\n', html_content)

    pattern = re.compile(r'</html>', re.IGNORECASE)
    html_content = re.sub(pattern, '', html_content)

    pattern = re.compile(r'<font.*?>', re.IGNORECASE | re.DOTALL)
    html_content = re.sub(pattern, '\n', html_content)

    pattern = re.compile(r'</font>', re.IGNORECASE)
    html_content = re.sub(pattern, '', html_content)

    pattern = re.compile(r'<br.*?>', re.IGNORECASE | re.DOTALL)
    html_content = re.sub(pattern, '', html_content)

    pattern = re.compile(r'<hr.*?>', re.IGNORECASE | re.DOTALL)
    html_content = re.sub(pattern, '', html_content)
    
    pattern = re.compile(r'<B>', re.IGNORECASE | re.DOTALL)
    html_content = re.sub(pattern, '\n', html_content)

    pattern = re.compile(r'</B>', re.IGNORECASE)
    html_content = re.sub(pattern, '', html_content)

    pattern = re.compile(r'<center>', re.IGNORECASE | re.DOTALL)
    html_content = re.sub(pattern, '\n', html_content)

    pattern = re.compile(r'</center>', re.IGNORECASE)
    html_content = re.sub(pattern, '', html_content)

    pattern = re.compile(r'<a.*?>', re.IGNORECASE | re.DOTALL)
    html_content = re.sub(pattern, '\n', html_content)

    pattern = re.compile(r'</a>', re.IGNORECASE)
    html_content = re.sub(pattern, '', html_content)

    pattern = re.compile(r'<table.*?>', re.DOTALL | re.IGNORECASE)
    html_content = re.sub(pattern, '\n', html_content)

    pattern = re.compile(r'</table>', re.DOTALL | re.IGNORECASE)
    html_content = re.sub(pattern, '', html_content)

    pattern = re.compile(r'<tr.*?>', re.DOTALL | re.IGNORECASE)
    html_content = re.sub(pattern, '\n', html_content)

    pattern = re.compile(r'</tr>', re.DOTALL | re.IGNORECASE)
    html_content = re.sub(pattern, '', html_content)

    pattern = re.compile(r'<td.*?>', re.DOTALL | re.IGNORECASE)
    html_content = re.sub(pattern, '\n', html_content)

    pattern = re.compile(r'</td>', re.DOTALL | re.IGNORECASE)
    html_content = re.sub(pattern, '', html_content)

    return html_content

def clean_lines(text_content): # Removes all lines that are empty/contain only whitespace and removes leading whitespace from the remaining lines
    """
    Drop empty lines and removes all leading and trailing whitespace.
    """
    cleaned_lines = [line.lstrip() for line in text_content.splitlines() if line.strip()]
    return '\n'.join(cleaned_lines)

def prepend_newline_to_p(html_content): # Finds every <p> tag and inserts a newline character before it
    """
    Insert a newline before every <p ...> tag to improve downstream line-based parsing.
    """
    pattern = re.compile(r'<p.*?>', re.IGNORECASE)
    processed_text = re.sub(pattern, r'\n\g<0>', html_content)    
    return processed_text

def strip_all_html_tags(html_content): # Removes all HTML tags from a string.
    """
    Remove all HTML tags by deleting substrings matching '<...>'.
    """
    pattern = re.compile(r'<.*?>')
    clean_text = re.sub(pattern, '', html_content)
    return clean_text

def remove_xbrli_measure(html_content): # Uses regex to find and remove the entire <xbrli:measure> ... </xbrli:measure> block.
    """
    Remove <xbrli:*>...</xbrli:*> blocks (e.g., <xbrli:measure>...</xbrli:measure>).
    """
    pattern = re.compile(r'<xbrli:([a-zA-Z0-9_:]+).*?>.*?</xbrli:\1>', re.DOTALL | re.IGNORECASE)
    html_content = re.sub(pattern, '', html_content)
    return html_content

def get_from_sec_document(html_content: str) -> str:
    """
    Trim content to start at the <SEC-DOCUMENT> marker if present.
    """
    pattern = re.compile(r'<SEC-DOCUMENT>.*\Z', re.DOTALL)
    match = re.search(pattern, html_content)
    return match.group(0) if match else html_content

def get_content_before_sequence(html_content):
    """
    Keep content before the '<SEQUENCE>2' marker, if present.
    """
    pattern = re.compile(r'^.*?(?=<SEQUENCE>2)', re.DOTALL)
    match = re.search(pattern, html_content)
    return match.group() if match else html_content

def break_on_item_heads(text: str) -> str:
    """
    Insert a newline before detected 'Item <number>[suffix].' headings
    or 'Item <number> [text] <number>.
    """
    _HEAD_DETECT = re.compile(
        r'\s*items?\b\s*'
        r'\d+[A-Za-z]?'
        r'(?:\s*(?:and|to|through|-)\s*\d+[A-Za-z]?)*'
        r'\s*\.',re.IGNORECASE)
    out = []
    last = 0
    for m in _HEAD_DETECT.finditer(text):
        start = m.start()
        if start > 0 and text[start-1] != '\n':
            out.append(text[last:start])
            out.append('\n')
            last = start
    out.append(text[last:])
    s = ''.join(out)                     # <-- join the list!
    return re.sub(r'[ \t]+\n', '\n', s)  # tidy spaces before newlines

def clean_html(file_content):
    """
    Perform end-to-end HTML-to-text cleaning for SEC filing content.
    """
    cleaned = soft_unwrap_html_lines(file_content)
    cleaned = get_from_sec_document(cleaned)
    
    cleaned = get_content_before_sequence(cleaned)                          # cuts after <SEQUENCE>2
    cleaned = remove_head_with_regex(cleaned)
    
    cleaned = remove_style_with_regex(cleaned)
    cleaned = remove_id_with_regex(cleaned)
    cleaned = remove_align_with_regex(cleaned)
    
    cleaned = remove_part_1(cleaned)
    cleaned = unwrap_tags(cleaned)                                          # Removes useless tags
    cleaned = remove_xbrli_measure(cleaned)
    
    cleaned = loop_clean(cleaned)                                           # LOOP : empty tags

    cleaned = prepend_newline_to_p(cleaned)

    cleaned = strip_all_html_tags(cleaned)
    cleaned = remove_numeric_entities(cleaned)
    cleaned = soft_unwrap_html_lines(cleaned)
    cleaned = break_on_item_heads(cleaned)
    cleaned = clean_lines(cleaned)
    return cleaned

def print_clean_txt(html_path):
    """
    Load a filing, clean it, and return the cleaned text.
    """
    try:
        with open(html_path, 'r', encoding='utf-8') as file:
            file_content = file.read()
        cleaned = clean_html(file_content)
    except FileNotFoundError:
        print(f"Error: The file '{html_path}' was not found.")
    return cleaned


# --------------------------------------------------------------------------------------------------------------------
#                                                Cleaning 'Items'
# --------------------------------------------------------------------------------------------------------------------

def cleaning_items(html_content):
    """
    Normalize broken 'Item' headings that are split across lines.
    """
    html_content = merge_I_tem(html_content)
    html_content = ensure_space_after_item(html_content)
    html_content = merge_item_with_number_line(html_content)
    return merge_item_number_with_suffix(html_content)

def merge_I_tem(content: str) -> str: # Finds lines with 'I' & next line starts with 'tem' then merge them
    """
    Merge cases where 'I' appears alone on a line and the next line starts with 'tem'.

    Example:
      Line i:   "I"
      Line i+1: "tem 1. Business"
      -> "Item 1. Business"
    """
    lines = content.splitlines()  # split into lines without keeping '\n'
    new_lines = []
    i = 0

    while i < len(lines):
        # Make sure there *is* a next line to look at
        if (
            lines[i].strip() == "I" and
            i + 1 < len(lines) and
            lines[i + 1].lstrip().startswith("tem")
        ):
            merged_line = "I" + lines[i + 1].lstrip()  # e.g. "Item 1. ..."
            new_lines.append(merged_line)
            i += 2  # skip the next line because we've merged it
        else:
            new_lines.append(lines[i])
            i += 1
    return "\n".join(new_lines)

def ensure_space_after_item(text: str) -> str:
    """
    Ensure 'Item' or 'Items' is followed by a space when immediately followed by non-space.

    Example:
      'Item1A' -> 'Item 1A'
    """
    return re.sub(r'\b(Items?)\b(?=\S)', r'\1 ', text)

def merge_item_with_number_line(text: str) -> str: # If a line is just 'Item'/'Items' and the following line starts with a number merges them
    """
    Merge lines where 'Item'/'Items' is on its own line and the next line begins with a digit.

    Example:
      "Item"
      "1. Business"
      -> "Item 1. Business"
    """
    lines = text.splitlines()
    new_lines = []
    i = 0

    while i < len(lines):
        current = lines[i].strip()

        # Check if this line is exactly 'Item' or 'Items'
        if current in ("Item", "Items") and i + 1 < len(lines):
            next_raw = lines[i + 1]
            # Remove leading spaces to inspect the first real character
            next_stripped_leading = next_raw.lstrip()

            # Check if next line starts with a digit
            if next_stripped_leading and next_stripped_leading[0].isdigit():
                # Merge: 'Item' + space + next line (without leading spaces)
                merged = f"{current} {next_stripped_leading}"
                new_lines.append(merged)
                i += 2  # skip the next line (already merged)
                continue

        # Default: keep line as-is
        new_lines.append(lines[i])
        i += 1

    return "\n".join(new_lines)

def merge_item_number_with_suffix(text: str) -> str:
    """
    If a line is 'Item {number}' only, and the following line starts with either:
      - a single letter and a dot (e.g., 'A. Risk Factors')
      - or just a dot (e.g., '. Risk Factors')
    then merge them into one line: 'Item 1A. Risk Factors' or 'Item 1. Risk Factors'.
    """
    lines = text.splitlines()
    new_lines = []
    i = 0

    while i < len(lines):
        current_stripped = lines[i].strip()

        # Match 'Item {number}' (e.g., 'Item 1', 'Item 12')
        if re.fullmatch(r'Item\s+\d+', current_stripped) and i + 1 < len(lines):
            next_raw = lines[i + 1]
            next_stripped = next_raw.lstrip()

            # Next line starts with 'A.' or 'b.' etc, OR with just '.'
            if re.match(r'[A-Za-z]\.', next_stripped) or next_stripped.startswith('.'):
                merged = current_stripped + next_stripped  # e.g. 'Item 1' + 'A. Risk Factors'
                new_lines.append(merged)
                i += 2
                continue

        # Default: keep line as-is
        new_lines.append(lines[i])
        i += 1
    return "\n".join(new_lines)

# --------------------------------------------------------------------------------------------------------------------
#                                              MERGES THE FUNCTIONS
# --------------------------------------------------------------------------------------------------------------------

def print_10X(full_path, html_content, output_filename):
    """
    Write cleaned filing text to disk.
    """
    with open(full_path, "w", encoding='utf-8') as new_file:
        new_file.write(html_content)
    print("\nCleaned content saved in {}".format(output_filename))

def cleaner(cik, output_filename):
    """
    Clean all downloaded 10-K filings for a given CIK folder and write outputs.

    This function:
      - locates the 10-K folder under SEC_DIR/<ticker>/10-K,
      - iterates over subdirectories (each filing),
      - reads 'full-submission.txt',
      - runs HTML cleaning + item-heading normalization,
      - writes the cleaned text to `output_filename` inside each filing directory.
    """
    folders_path = SEC_DIR / cik / "10-K"
    for p in folders_path.iterdir():
        print(p)
        full_path = os.path.join(p, output_filename)
        html_content = print_clean_txt(full_path)                    # html removal
        html_content = cleaning_items(html_content)
        print_10X(full_path, html_content, output_filename)
    return
