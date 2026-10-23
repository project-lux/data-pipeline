# 11. Mapper hooks: `make_export_filename`, `extract_index_data`

**Priority:** P1 · **Size:** S · **Depends on:** 01 · **Unblocks:** 12, 26
**Read from:** `rob_refactor:lux_pipeline/process/base/mapper.py` — only the two methods and the `logger` lines

## Intent

Add two small extension points to `lux_pipeline/process/base/mapper.py`:

```python
def make_export_filename(self, name, my_slice):
    return f"export_{name}_{my_slice}.jsonl"

def extract_index_data(self, record):
    """From a datacache record, return {"label": [(key, value), ...], "equiv": [...], "diff": [...]}
    for the loader to write to per-slice TSV files during a load. Default: nothing."""
    return {}
```

`extract_index_data` is the hook the DSL loader (item 12) calls after storing
each record, which is how the wikidata/viaf loaders' hand‑written "write
equivs CSV while loading" logic can move into the mapper later.
`make_export_filename` is what the export manager (item 26) asks the output
mapper for.

## Files

| Action | Path on `main` |
|---|---|
| Edit | `lux_pipeline/process/base/mapper.py` |
| Edit | `lux_pipeline/sources/lux/marklogic/mapper.py` — override `make_export_filename` to return today's names (`export_full_{n}.jsonl`) |
| Edit | `lux_pipeline/sources/lux/qlever/mapper2.py` — override to `sources_{n}.nt` style used by `run-source-build.py` |

## What to keep from `main`

Everything. In particular the century‑regex code (`match.group(2)` with the
diagnostic `try/except`) — the refactor's version is older and wrong.

## Steps

1. Add the two methods to `Mapper`, with docstrings.
2. Add the two overrides.
3. No new tests beyond a one‑liner in an existing mapper test asserting the default returns `{}`.

## Acceptance criteria

* `python -m pytest tests/ -q` passes; no behaviour change.
