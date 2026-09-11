"""Streaming reads over the OpenAlex parquet snapshot.

Scans a large entity, keeps rows matching a set of identifiers, and projects
away unused columns. Column projection reads roughly 16% of the compressed
bytes of the works entity. Partitions are read one file at a time to bound peak
memory and allow the scan to resume.

The snapshot directory is Hive-partitioned on `updated_date`, and the files also
carry `updated_date` as a timestamp column, so it cannot be opened as a single
pyarrow dataset. Files are opened individually with `pq.ParquetFile`.

The snapshot path is read from `snapshot_config.py` (see
`snapshot_config.example.py`).

Usage:

    from snapshot import Snapshot

    snap = Snapshot()
    rows = snap.scan_works(
        columns=["id", "doi", "publication_year", "authorships"],
        row_filter=lambda t: ...,
    )
"""

from __future__ import annotations

import glob
import os
import time
from typing import Callable, Iterable, Iterator, Sequence

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

try:
    from snapshot_config import SNAPSHOT_ROOT
except ImportError:
    raise SystemExit(
        "snapshot_config.py not found. Copy snapshot_config.example.py to "
        "snapshot_config.py and set SNAPSHOT_ROOT to the local snapshot path.")

GIB, MIB = 2 ** 30, 2 ** 20


def strip_id(x):
    """Return a bare OpenAlex id, dropping the URL prefix if present."""
    if x is None:
        return None
    s = str(x)
    return s.rsplit("/", 1)[-1] if s.startswith("http") else s


def strip_doi(x):
    """Normalise a DOI to bare lowercase form, or None.

    Non-string inputs and values that do not resolve to a `10.` prefix return
    None, so placeholder and missing values do not enter the wanted set.
    """
    if not isinstance(x, str):
        return None
    s = x.strip().lower()
    for p in ("https://doi.org/", "http://doi.org/", "doi:"):
        if s.startswith(p):
            s = s[len(p):].strip()
    return s if s.startswith("10.") else None


def _flatten_author_ids(col: pa.ChunkedArray | pa.Array):
    """Flatten a list<struct> authorships column into (author_id, parent_row).

    Returns a StringArray of author ids and an Int64Array of the row each came
    from, both the same length. Comparison is done on the flat array in Arrow
    and mapped back via the parent indices.
    """
    if isinstance(col, pa.ChunkedArray):
        col = col.combine_chunks()

    values = col.values
    if len(values) == 0:
        return pa.array([], type=pa.string()), pa.array([], type=pa.int64())

    parent = pc.list_parent_indices(col)
    ids = values.field("author").field("id")

    # Ids keep their URL prefix here; the caller prefixes its wanted set.
    return ids, parent


class Snapshot:
    def __init__(self, root: str = SNAPSHOT_ROOT, verbose: bool = True):
        self.root = root
        self.verbose = verbose
        if not os.path.isdir(root):
            raise SystemExit(f"snapshot not found at {root}")

    # ------------------------------------------------------------------ files

    def files(self, entity: str) -> list[str]:
        """Every parquet file for an entity, in partition order."""
        pat = os.path.join(self.root, entity, "**", "*.parquet")
        return sorted(glob.glob(pat, recursive=True))

    def describe(self, entity: str) -> dict:
        fs = self.files(entity)
        total = sum(os.path.getsize(f) for f in fs)
        parts = {os.path.basename(os.path.dirname(f)) for f in fs}
        return {"files": len(fs), "bytes": total, "partitions": len(parts)}

    def columns(self, entity: str) -> list[str]:
        fs = self.files(entity)
        if not fs:
            return []
        return list(pq.ParquetFile(fs[0]).schema_arrow.names)

    # ------------------------------------------------------------------- scan

    def scan(
        self,
        entity: str,
        columns: Sequence[str],
        handler: Callable[[pa.Table, str], object],
        batch_rows: int = 200_000,
        limit_files: int | None = None,
        skip_files: int = 0,
        progress_every: int = 100,
    ):
        """Stream one entity, calling handler on each batch.

        handler receives (table, path) and returns whatever the caller wants
        accumulated; returning None accumulates nothing. Reads in batches so a
        large partition need not fit in memory at once. limit_files and
        skip_files restrict the scan for validation runs.
        """
        fs = self.files(entity)
        if skip_files:
            fs = fs[skip_files:]
        if limit_files is not None:
            fs = fs[:limit_files]
        if not fs:
            raise SystemExit(f"no parquet files for {entity} under {self.root}")

        available = set(self.columns(entity))
        missing = [c for c in columns if c not in available]
        if missing:
            raise SystemExit(
                f"{entity} has no column(s) {missing}. Available: "
                f"{sorted(available)[:20]} ...")

        total_bytes = sum(os.path.getsize(f) for f in fs)
        if self.verbose:
            print(f"scanning {entity}: {len(fs):,} files, "
                  f"{total_bytes / GIB:.1f} GiB on disk")
            print(f"  projecting {len(columns)} of {len(available)} columns")

        results = []
        n_rows = n_kept = 0
        done_bytes = 0
        t0 = time.time()

        for i, path in enumerate(fs):
            try:
                pf = pq.ParquetFile(path)
                for batch in pf.iter_batches(batch_size=batch_rows,
                                             columns=list(columns)):
                    tbl = pa.Table.from_batches([batch])
                    n_rows += tbl.num_rows
                    out = handler(tbl, path)
                    if out is not None:
                        results.append(out)
                        n_kept += (len(out) if hasattr(out, "__len__") else 1)
            except Exception as e:
                print(f"  [!] {os.path.basename(path)}: {str(e)[:90]}")

            done_bytes += os.path.getsize(path)
            if self.verbose and (i + 1) % progress_every == 0:
                el = time.time() - t0
                rate = done_bytes / el / MIB if el else 0
                eta = (total_bytes - done_bytes) / (done_bytes / el) if done_bytes else 0
                print(f"  {i + 1:,}/{len(fs):,} files | "
                      f"{n_rows / 1e6:.1f}M rows | kept {n_kept:,} | "
                      f"{rate:.0f} MiB/s | eta {eta / 60:.0f}m", flush=True)

        if self.verbose:
            el = time.time() - t0
            print(f"  done: {n_rows:,} rows scanned, {n_kept:,} kept, "
                  f"{el / 60:.1f} min")
        return results

    # ------------------------------------------------- convenience for works

    def works_for_authors(
        self,
        author_ids: Iterable[str],
        columns: Sequence[str] | None = None,
        **kw,
    ):
        """Every work with at least one of these authors.

        Returns a list of Tables, each holding the matching rows from one batch.
        Exploding `authorships` is left to the caller.
        """
        want = set(strip_id(a) for a in author_ids)
        if self.verbose:
            print(f"  matching against {len(want):,} author ids")

        cols = list(columns or ["id", "doi", "publication_year", "type",
                                "is_retracted", "cited_by_count",
                                "counts_by_year", "authorships",
                                "primary_location"])
        if "authorships" not in cols:
            cols.append("authorships")

        # Match against both prefixed and bare forms; stripping every id in the
        # data would remove the projection saving.
        PREFIX = "https://openalex.org/"
        want_arr = pa.array(sorted(want | {PREFIX + w for w in want}),
                            type=pa.string())

        def handler(tbl, path):
            ids, parent = _flatten_author_ids(tbl.column("authorships"))
            if len(ids) == 0:
                return None
            hit = pc.is_in(ids, value_set=want_arr)
            rows = pc.unique(pc.filter(parent, hit))
            if not len(rows):
                return None
            return tbl.take(rows.sort())

        return self.scan("works", cols, handler, **kw)

    def works_for_dois(
        self,
        dois: Iterable[str],
        columns: Sequence[str] | None = None,
        **kw,
    ):
        """Works whose DOI is in the given set."""
        want = set(strip_doi(d) for d in dois if d)
        want.discard(None)
        if self.verbose:
            print(f"  matching against {len(want):,} dois")

        cols = list(columns or ["id", "doi", "publication_year",
                                "publication_date", "is_retracted",
                                "authorships", "primary_location",
                                "cited_by_count"])
        if "doi" not in cols:
            cols.append("doi")

        # Match against both prefixed and bare forms in a single Arrow pass.
        PREFIX = "https://doi.org/"
        want_arr = pa.array(sorted(want | {PREFIX + w for w in want}),
                            type=pa.string())

        def handler(tbl, path):
            col = tbl.column("doi")
            if isinstance(col, pa.ChunkedArray):
                col = col.combine_chunks()
            hit = pc.is_in(pc.utf8_lower(col), value_set=want_arr)
            rows = pc.indices_nonzero(pc.fill_null(hit, False))
            return tbl.take(rows) if len(rows) else None

        return self.scan("works", cols, handler, **kw)


def concat(tables: list[pa.Table]) -> pa.Table | None:
    """Combine scan results into one table."""
    tables = [t for t in tables if t is not None and t.num_rows]
    return pa.concat_tables(tables) if tables else None


if __name__ == "__main__":
    snap = Snapshot()
    for e in ["works", "authors", "sources"]:
        d = snap.describe(e)
        print(f"{e:<10}{d['files']:>7,} files{d['bytes'] / GIB:>9.1f} GiB"
              f"{d['partitions']:>8,} partitions")
