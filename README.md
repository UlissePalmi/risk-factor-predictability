# Risk Factor Predictability

This repository builds a panel dataset of 10-K Item 1A risk factor disclosure changes and tests whether disclosure change features predict future returns using Random Forest regression and classification.

The project runs as a step-based pipeline, from constructing a CIK universe and downloading SEC filings, to text extraction, feature computation, return merging, panel construction, and model estimation.

## What the pipeline does

0. Finds list of CIK from SEC website
1. **Download**: downloads historical 10-K filings from SEC EDGAR and store them locally
2. **Clean**: cleans raw filings into consistent text
3. **Extract Item 1A**: splits filings into sections, extracts Item 1A sections for feature construction
4. **Feature Engineering**: computes firm-level year-over-year disclosure-change features by comparing each firm’s consecutive Item 1A sections
5. **Retreives Returns**: retrieves monthly stock returns from wrds database
6. **Dataset build**: merges disclosure-change features with return data to create a model ready panel
7. **Modeling**: runs baseline predictive models and evaluation to quantify whether disclosure changes contain incremental predictive information


## Project structure (high-level)

risk_factor_pred/
  edgar/ — SEC index + filing download utilities
  pipeline/steps.py — step definitions for the full pipeline
  model/ modules (RF setup, regression, classification)
  config.py — project directories, constants, output file paths
scripts/
  99_reproduce_all.py — main entry point to run the pipeline end-to-end


## Notes on scope and reproducibility

- This repository is a research codebase intended for end-to-end reproduction of the pipeline (download → parse → similarity measures → dataset → baseline models).
- The code follows a `src/` layout to support stable imports and testing; modules are intended to be executed from the repository root.
- Data artifacts (SEC downloads and derived datasets) are not committed to version control due to size and data-handling constraints. The pipeline writes outputs to `data/` and `outputs/` as described below.

---

## Quickstart

### 1) Setup environment (uv)

From the repository root:

```powershell
uv sync
```

### 2) Run modules (recommended)

Because the project uses package-style imports, run scripts using `python -m ...` from the repo root.

Example:

```powershell
uv run python -m quant_project.DataSetup_10X.sec_download_html
```

---

## Data layout

All data artifacts should live under `data/` and are generally excluded from version control. Recommended structure:

```
data/
  html/
    sec-edgar-filings/  # SEC downloader outputs (HTML and metadata)
  processed/
    datasets/           # model-ready merged datasets
  external/
    master/             # manually maintained inputs (e.g., master lists)
  samples/
    ...                 # small public/demo inputs (optional, can be committed)
outputs/
  runs/                 # metrics, logs, figures per run (not committed)
```

### Where specific artifacts go

- SEC HTML downloads: `data/raw/sec/`
- cleaned/split sections: `data/interim/sections/`
- similarity features (e.g., `similarity_data.csv`): `data/interim/similarity/`
- merged modeling dataset: `data/processed/datasets/`
- model outputs / figures: `outputs/runs/<timestamp>/`

### Note on version control
Do **not** commit large or regenerable artifacts (`data/raw`, `data/interim`, `data/processed`, `outputs`). If you want reviewers to run a lightweight demo, include a small, non-proprietary sample under `data/samples/`.

---

## Pipeline overview

### Step A — Download SEC filings
Downloads 10-K filings via EDGAR. This creates a local folder of filings (HTML and/or text).  
**Output:** `data/raw/sec/...`

Typical command:

```powershell
uv run python -m quant_project.DataSetup_10X.sec_download_html
```

### Step B — Clean and split filing sections
Parses filings and extracts target sections such as Risk Factors (Item 1A), etc.  
**Output:** `data/interim/sections/...`

Example:

```powershell
uv run python -m quant_project.Splitter_10X.fx_splitter_10X
```

### Step C — Compute similarity features
Computes similarity metrics between consecutive years’ sections.  
**Output:** `data/interim/similarity/similarity_data.csv`

Example:

```powershell
uv run python -m quant_project.Similarity_10X.fx_similarity
```

### Step D — Merge with returns and build model-ready dataset
Merges text features with return data and builds a modeling dataset.  
**Output:** `data/processed/datasets/<dataset>.csv` (or parquet)

Example:

```powershell
uv run python -m quant_project.ML_Model.merger
```

### Step E — Train baseline models
Trains baseline models and reports metrics.  
**Output:** `outputs/runs/...`

Example:

```powershell
uv run python -m quant_project.Models.model
```

---

## How to run an end-to-end workflow

If you prefer a single “driver” module, run your orchestration entry point (if applicable):

```powershell
uv run python -m quant_project.main
```

If you orchestrate via scripts, consider placing them under `scripts/` and running as a module:

```powershell
uv run python -m scripts.cluster1
```

If you keep orchestration at repo root, run from repo root and ensure imports use `quant_project.*` (not old flat folder names).

---

## Project structure

```
src/
  quant_project/
    DataSetup_10X/       # EDGAR download + raw data handling
    Splitter_10X/        # section extraction / splitting
    Similarity_10X/      # similarity feature computation
    ML_Model/            # merging / label construction utilities
    Models/              # baseline training and evaluation
    createList/          # helpers for building firm lists (CIK mappings, etc.)
pyproject.toml
uv.lock
README.md
```

---

## Reproducibility notes

- Use module execution (`python -m ...`) from the repo root to avoid import path issues.
- `uv.lock` pins resolved dependency versions; `uv sync` reproduces the environment.
- If you add randomness (train/test split, model seeds), set a fixed `random_state` / seed and log it to `outputs/`.

---

## Troubleshooting

### “attempted relative import with no known parent package”
You are running a file directly. Use module execution:

```powershell
uv run python -m quant_project.<submodule>.<module>
```

### “ModuleNotFoundError” for a sibling file
Inside a package, use explicit package imports, e.g.:

```python
from quant_project.DataSetup_10X import fx_10X_cleaner as fc
```

or relative imports if the module is executed as part of the package:

```python
from . import fx_10X_cleaner as fc
```

### WRDS authentication issues
If you use WRDS, expect interactive authentication unless you configure credentials. Do not hard-code usernames/tokens in code. Prefer environment variables or prompts.

---

## Data access and licensing

- SEC EDGAR data is public; respect SEC rate limits and include appropriate headers/user-agent where required.
- WRDS data is licensed; do not commit raw WRDS extracts to a public repository.

---

## Development

Install dev tools:

```powershell
uv sync --all-extras
```

Run lint/tests (if configured):

```powershell
uv run ruff check .
uv run pytest
```

---
