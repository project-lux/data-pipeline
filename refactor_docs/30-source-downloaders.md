# 30. Source downloaders

**Priority:** P2 · **Size:** S · **Depends on:** 14, 15
**Read from:** `rob_claude_refactor:lux_pipeline/sources/ror/downloader.py`, `rob_refactor:lux_pipeline/sources/{viaf,yale/library}/downloader.py`, `rob_refactor:lux_pipeline/sources/pleiades/downloader.py`

## Intent

Add the per‑source downloaders that discover their real URL from a
metadata page, plus fix the one already on `main`.

| `main` path | Action | Config keys |
|---|---|---|
| `ror/downloader.py` | create from `rob_claude_refactor` (`RorDownloader`: Zenodo JSON → `hits/hits[0]/files[0]/links/self`; already has `get_urls(type=…)` and the example URLs in a comment) | `downloadMdUrl`, `downloadXPath`, `dumpFilePath` |
| `oclc/downloader.py` | create (`ViafDownloader`: `downloadUrlBase` + path from JSON) | `downloadMdUrl`, `downloadXPath`, `downloadUrlBase` |
| `pleiades/downloader.py` | edit: `get_urls(self)` → `get_urls(self, type="all")`; `self.config['dumps_dir']` → `self.dumps_dir` (the base resolves it; the config key is optional) | `input_files` |
| `yale/library/downloader.py` | create `S3Downloader` stub returning `[]` **only if** a config will reference it; otherwise skip | — |

Set `downloaderClass` in `docs/sample_config/ror.json`, `viaf.json`
(oclc), `pleiades` sample (create if missing).

## Known bugs in the source — do not copy

* All refactor `get_urls(self)` signatures lack the `type` parameter the
  manager passes (`get_urls(self.download_type)`) — they would raise
  `TypeError`. Add `type="all"`.
* `RorDownloader` uses `url2.rsplit('/')[-2]` for the filename (Zenodo
  URLs end in `/content`); keep but document.

## Acceptance criteria

* `lux download --source ror --no-ui` fetches the current ROR zip to `dumps_dir`.
* `lux_pipeline.sources.pleiades.downloader` works under `lux download --source pleiades`.
