# 15. `DownloadManager` and `lux download`

**Priority:** P1 · **Size:** S · **Depends on:** 07, 14 · **Unblocks:** 30, 31
**Read from:** `rob_refactor:lux_pipeline/process/tasks/download_task.py` (51 lines), `rob_refactor:lux_pipeline/cli/download.py` (21 lines)

## Intent

```
lux download --source all --type records --max-workers 10
lux download --source wikidata                 # one 100 GB file, one bar
lux download --source all --type export        # the published LUX export set
```

`DownloadManager.maybe_add` asks each source's downloader for its URLs
(`get_urls(download_type)`) and flattens them into `self.sources` as
`(which, name, url_dict)`; `_distributed(n)` takes every `max_workers`‑th
entry. One bar per worker shows the current file.

## Files

| Action | Path on `main` | Source |
|---|---|---|
| Create | `lux_pipeline/process/tasks/download_task.py` | `rob_refactor:lux_pipeline/process/tasks/download_task.py` |
| Create | `lux_pipeline/cli/download.py` | `rob_refactor:lux_pipeline/cli/download.py` |
| Create | `tests/test_download_task.py` | new |

## What to port

Both files. Changes:

* No bar handling in the manager. Expose `idle_workers()` →
  `range(len(self.sources), self.max_workers)`; the `CommandHandler` (item 05)
  calls `view.set_visible(n, False)` for those.
* `_distributed(self, n, cfgs, progress)`: `dl.prepare(n, self.max_workers, progress=progress)`
  then `dl.process(url, overwrite=self.overwrite)` (item 14 adds the parameter).
  `download_type` and `overwrite` are read from `args` in `__init__`.
* `_distributed` must not `raise` after logging — log, count the failure,
  continue with the next URL, and return the failure count so the driver
  can exit non‑zero (`process()` returns the per‑worker results, item 07).

## Steps

1. Create both files.
2. Test with a stub downloader whose `get_urls` returns three entries and
   `max_workers=2`; `run_single(0)` → worker 0 sees entries 0 and 2.
3. Live: `lux download --source ror --no-ui` fetches to `dumps_dir`.

## Acceptance criteria

* Tests pass; a failing URL does not abort the others.
