# 04. Config: system record, defaults, paths, downloader class

**Priority:** P0 · **Size:** M · **Depends on:** 01 · **Unblocks:** 05, 12, 14, 35
**Read from:** `rob_claude_refactor:lux_pipeline/config.py` (the whole file, ~640 lines) and `main:pipeline/config.py`

## Intent

Bring the *compatible* improvements of the refactored `Config` onto `main`'s
`lux_pipeline/config.py` without changing how the live configuration directory is
read:

1. `importObject` via `importlib`, auto‑prefixing `lux_pipeline.` (item 00 already
   switched the prefix and added the legacy `pipeline.` mapping) and
   logging the real `ModuleNotFoundError`.
2. A `system` record is read when present, else `base` (today's name). Same
   for `defaults` (`{"type":"default","name":"cache"|"map"|"ml"}`) falling
   back to today's single `caches` record.
3. `handle_config_record(k, rec)` dispatch method (so item 35 can reuse it).
4. New base settings with defaults: `max_workers` (default
   `cpu_count() - 3`, min 1), `temp_dir`, `indexes_dir` made absolute like
   the other `*_dir` values (`main` already does `temp_dir`; add
   `indexes_dir`).
5. Per‑source `downloaderClass` → `cfg["downloader"]` (default
   `process.base.downloader.BaseDownloader` once item 14 lands; until then
   default `None`).
6. `merge_configs(cfg, defaults)` applied to map and marklogic stores too
   (`instantiate_map`, `instantiate_ml`), not only caches.
7. Unknown record `type` logs at CRITICAL instead of printing.

## Why it matters

"Better configuration management and division" is one of the refactor's
goals. The division the refactor made — machine‑specific settings
(`system`) separate from pipeline semantics, and per‑kind defaults instead of
one `caches.json` — is sound, but it was done by *renaming* the records, which
is why the refactor cannot read a `main` config directory and vice versa.
Reading both names makes the split available without a migration.

## Files

| Action | Path on `main` | Source |
|---|---|---|
| Edit | `lux_pipeline/config.py` | `rob_claude_refactor:lux_pipeline/config.py` |
| Edit | `docs/sample_config/base.json` — add `"max_workers"`, `"temp_dir": "tmp"`, comment the new optional keys | |
| Create | `tests/test_config_records.py` | new |

## What to port (precisely)

From `rob_claude_refactor` `Config.__init__` / helpers:

* `importObject`: the `importlib.import_module` version. Keep `main`'s
  behaviour of trying the bare path first, then `lux_pipeline.<path>`, then the
  legacy `pipeline.<path>` rewrite from item 00. Log
  `ModuleNotFoundError` with `logger.critical` before re‑raising.
* `self.defaults = {"cache": {}, "map": {}, "marklogic": {}}` populated from
  `"type": "default"` records keyed by `name`; after the loop, if
  `self.caches` (today's record) is non‑empty and `defaults["cache"]` is
  empty, set `defaults["cache"] = self.caches`. Keep the `self.caches`
  attribute so nothing else breaks.
* `base = configcache["system"] or configcache["base"]` with a clear error
  if neither exists.
* `handle_config_record`.
* `max_workers`, `indexes_dir` handling.
* In `instantiate()`: the `downloaderClass` block; `merge_configs` for
  `recordcache2`/`reconciledRecordcache` already exists — leave it.

## What to keep from `main` (do not overwrite)

* `URICache`‑backed `canonicalize()` and `build_match_index()` /
  `_split_uri_scan` / `_finish_split`. The refactor's `split_uri` is the old
  linear scan.
* `validatorClass` handling (the refactor commented it out). The sample value is `lux_pipeline.process.validator.Validator` after item 00; the legacy `pipeline.…` value in the live config is accepted by `importObject`.
* `instantiate()` **re‑raises** on failure. The refactor logs and continues
  (`# raise` commented out), which turns a bad class path into a `None`
  component and a confusing `AttributeError` later. Keep raising.
* `self.caches` attribute and the `"caches"` record type.
* The `"indexes"` block in `instantiate()` is **not** part of this item (item 34).
* Sub‑config stores (`"type": "config"`, `instantiate_config`) are item 35.

## Known bugs in the source — do not copy

* `rob_refactor` `instantiate_config` re‑resolves `base_dir` etc. with
  `else: cfg[d] = getattr(self, d)` which overwrites absolute paths — not
  needed here.
* `fix_identifier` in both branches is identical; leave it.

## Steps

1. Apply the seven points above to `lux_pipeline/config.py` in that order; each
   is a few lines.
2. `tests/test_config_records.py`: build a `Config` from a temporary
   directory containing (a) `base.json` + `caches.json` (today's shape) and
   (b) `system.json` + `default_cache.json` (`{"type":"default","name":"cache",…}`).
   Use `FsCache` as the config cache (that is what `Config(basepath=…)`
   does). Assert both load, `defaults["cache"]` is populated in both,
   `max_workers >= 1`, `indexes_dir` is absolute, and an unknown `type`
   does not raise. No Postgres is needed as long as you do not call
   `instantiate_all()`.
3. Load the live instance: `python -c "from lux_pipeline.runtime import get_configs; get_configs()"` (item 03) or the eight‑line bootstrap from `run-identify.py`.

**Live config copies.** `config_cache/config_cache/` is an untracked copy of the
build machine's configuration files (see README ground rule 5). In the same PR,
apply this item's config changes there: the new `base.json` keys (`max_workers`, `temp_dir`) and any `"type": "default"` records you introduce; the existing `caches.json` record keeps working, so only add what the item needs. Check the edited files still
parse as JSON and that every `*Class` value resolves through `importObject`
without a legacy warning, and list the edited files in the PR description.
Leave the local instance alone.

## Acceptance criteria

* The live config directory loads unchanged.
* A directory using `system.json` + `default` records also loads.
* `cfgs.max_workers`, `cfgs.temp_dir`, `cfgs.indexes_dir` exist on every load.
* Tests pass.
