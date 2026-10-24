# 14. Downloader base class

**Priority:** P1 · **Size:** S · **Depends on:** 02, 04 · **Unblocks:** 15, 30
**Read from:** `rob_claude_refactor:lux_pipeline/process/base/downloader.py` (≈145 lines)

## Intent

Add `lux_pipeline/process/base/downloader.py` with `BaseDownloader(Sliced)` (item 02):

* `get_urls(type="all") -> [{"url", "path"}, …]` — from `remoteDumpFile` +
  `dumpFilePath`, and from `input_files[<type>]` entries (same config shape
  the loader reads, item 12). Bare filenames resolve against `dumps_dir`.
* `process(download, overwrite=True)` — streams one URL to its path,
  reporting `self.progress(total=content_length, description=filename)` and
  `self.progress(advance=len(chunk))`.
* Helpers for "splash page" sources whose real URL must be discovered:
  `fetch_webpage(url)`, `get_value_from_json(url, dotted.key[0].path)`;
  `get_value_from_html`/`get_value_from_xml` stubs.

## Why it matters

Fourth headline feature. Today `docs/rebuild.md` says "download them …
using curl or wget". `main` already contains
`lux_pipeline/sources/pleiades/downloader.py`, which imports
`lux_pipeline.process.base.downloader.BaseDownloader` — a module that does not
exist on `main`. This item makes that file importable.

## Files

| Action | Path on `main` | Source |
|---|---|---|
| Create | `lux_pipeline/process/base/downloader.py` | `rob_claude_refactor:lux_pipeline/process/base/downloader.py` |
| Edit | `lux_pipeline/config.py` — default `downloaderClass` to `process.base.downloader.BaseDownloader` (item 04 left it `None`) | |
| Create | `tests/test_downloader.py` | new |

## What to port

The whole module. Changes:

* `requests.get(url, stream=True, verify=False)` → `verify=self.config.get("download_verify_ssl", True)`.
  Never default to unverified TLS.
* Download to `path + ".part"` and `os.replace` on success, so a killed
  download does not leave a truncated file that the loader then reads.
* Honour `overwrite=False`: skip when the target exists and
  `--no-overwrite` was given (the CLI already has the flag; the base ignores it).
* Remove the two `time.sleep()` calls at the end of `process`; replace
  `self.manager.update_progress_bar(...)`/`self.manager.log(...)` with
  `self.progress(...)`/`logger`.
* Add `timeout=(10, 60)` to `requests.get`.
* `get_urls()` must return `[]` (not raise) for a source with neither key, so
  `DownloadManager.maybe_add` can skip it.

## Steps

1. Create the module; module docstring explains the config keys
   (`remoteDumpFile`, `dumpFilePath`, `input_files`, `dumps_dir`,
   `download_verify_ssl`).
2. Set the config default (item 04 left a TODO).
3. `tests/test_downloader.py`: `get_urls` from the three config shapes;
   `get_value_from_json` on a canned dict via `monkeypatch` of
   `fetch_webpage`; `process` against a local `http.server` thread serving
   a 1 MB file into `tmp_path`, asserting the `.part` rename and that a
   recording `progress` saw `total` and advances summing to the size.
4. `python -c "import lux_pipeline.sources.pleiades.downloader"` now succeeds.

**Live config copies.** `config_cache/config_cache/` is an untracked copy of the
build machine's configuration files (see README ground rule 5). In the same PR,
apply this item's config changes there: the downloader keys the module docstring defines (`remoteDumpFile`, `dumpFilePath`, `input_files`, `dumps_dir`, `download_verify_ssl`) for every source config that should gain them. Check the edited files still
parse as JSON and that every `*Class` value resolves through `importObject`
without a legacy warning, and list the edited files in the PR description.
Leave the local instance alone.

## Acceptance criteria

* Tests pass; the pleiades downloader imports.
* `cfgs.external["ror"]["downloader"]` exists on the live instance (base class).
