# retraction-attribution-penalty

Replication code and derived data for *Post-retraction publishing activity
across notice-attribution categories*.

The study links the Retraction Watch database to a fixed OpenAlex snapshot and
follows 17,294 authors retracted between 2015 and 2019 against 19,259 matched
never-retracted authors, estimating how post-retraction publishing activity
varies across three notice-attribution categories (author misconduct, honest
error, editorial compromise) and by byline position.

## Repository layout

```
src/
  snapshot.py                            # streaming reader for the OpenAlex parquet snapshot
  snapshot_config.example.py             # template for the local snapshot path
notebooks/
  phase01_taxonomy.ipynb                 # 112 reason labels -> attribution categories
  phase02_doi_merge.ipynb                # DOI merge, author extraction, byline position
  phase03_author_screening.ipynb
  phase04_treated_histories.ipynb
  phase05_control_candidates.ipynb
  phase06_matching.ipynb                 # exact + nearest-neighbour matching
  phase07_control_histories.ipynb
  phase08_panel_treated.ipynb            # treated author-year panels
  phase09_estimation_treated.ipynb       # treated-only event studies, byline, placebo
  phase10_heterogeneity.ipynb            # pre-declared subgroup tests
  phase11_panel_controls.ipynb           # combined panel with matched controls
  phase12_estimation_controls.ipynb      # estimates vs controls, equivalence tests
  phase13_callaway_santanna.ipynb        # staggered-adoption estimator
  figures.ipynb                          # manuscript figures
data/
  interim/                               # panels written by the pipeline
  results/                               # coefficient and test tables
figures/                                 # generated PNG and PDF
```

## Data sources

| Source | Access | Redistribution |
|---|---|---|
| Retraction Watch | Retrieved via Crossref | Referenced, not redistributed; extraction documented in code |
| OpenAlex | Snapshot dated 27 June 2026 | CC0; derived aggregates included |
| World Bank income classification | Historical classification by economy | Public |

The full OpenAlex snapshot (~675 GiB) is not included. `src/snapshot.py` reads
it from a local path; the derived author-year panels needed to reproduce the
results are provided under `data/`.

## Requirements

- Python 3.11 or later
- numpy, pandas, pyarrow
- matplotlib (figures only)

```
pip install -r requirements.txt
```

## Usage

1. Copy `src/snapshot_config.example.py` to `src/snapshot_config.py` and set
   `SNAPSHOT_ROOT` to the local OpenAlex snapshot path. This file is gitignored.
2. Run the notebooks in phase order. Phases 2, 4, 5, and 7 scan the full
   snapshot and are the slow steps; Phases 8 to 13 run in minutes from the
   written panels.
3. `figures.ipynb` reads the tables under `data/results/` and writes to
   `figures/`.

Estimation parameters (windows, calipers, equivalence bounds, random seed) are
set at the top of each notebook.

## Citation

If you use this code or data, please cite the paper. Full citation and DOI will
be added on publication.

## License

Derived data are released under CC BY 4.0. Retraction Watch and 
OpenAlex data remain subject to their respective terms.
