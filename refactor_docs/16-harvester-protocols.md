# 16. Harvester: protocol / driver split

**Priority:** P1 · **Size:** M · **Depends on:** 02, 04 · **Unblocks:** 17
**Read from:** `rob_refactor:lux_pipeline/process/base/harvester.py` (496 lines), `rob_refactor:lux_pipeline/sources/getty/harvester.py`; `main:pipeline/process/base/harvester.py` (≈300 lines), `main:pipeline/process/update_manager.py`

## Intent

Split `main`'s `ASHarvester`/`PmhHarvester` into two layers, as the
refactor did:

* **Protocol** (`HarvestProtocol` → `ASProtocol`, `PmhProtocol`,
  `GettyProtocol`): knows how to walk a feed and yields
  `(change, identifier, record, changeTime)`. Pure I/O over a feed; no
  caches, no idmap.
* **Driver** (`ASHarvester(Sliced)`, item 02): owns the protocol instance
  (`harvestProtocolClass` config, default `ASProtocol`), applies each change
  to the caches (`process_change`), supports two‑phase harvesting
  (`get_record_list()` writes `all_<name>_uris.txt`, then
  `harvest_from_list(n, max)` fetches records in parallel), reports progress.

## Why it matters

The current harvester mixes feed walking with cache writes, which is why the
only parallel mode is `harvest_pages` (page‑number striding). The split lets
one driver run any protocol and makes "list first, then fetch in parallel"
a built‑in mode.

## Files

| Action | Path on `main` | Source |
|---|---|---|
| Edit (restructure) | `lux_pipeline/process/base/harvester.py` | `rob_refactor:lux_pipeline/process/base/harvester.py` |
| Edit | `lux_pipeline/process/update_manager.py` | keep; see below |
| Edit | `lux_pipeline/sources/getty/harvester.py` | `rob_refactor:lux_pipeline/sources/getty/harvester.py` |
| Create | `tests/test_harvester_protocols.py` | new |

## What to port

From the refactor: the class split, `HarvestProtocol.prepare()`,
`set_page_cache()` (`activitystreams_cache_source` config key replaces the
hard‑coded `cfgs.external["activitystreams"]` in `run-harvest.py`),
`ASProtocol.crawl_single()` / `crawl()`, the per‑item progress increments,
`ASHarvester.prepare()/process()/get_record_list()/harvest_from_list()`,
and `GettyProtocol`.

## What to keep from `main` (do not overwrite)

* **Delete handling.** `main`'s `UpdateManager.process_change` records
  pending deletes (`_record_pending_delete`, `process_pending_deletes`).
  The refactor's `ASHarvester.process_change` is the *old* version with
  `pass` placeholders. Make the driver's `process_change` delegate to
  `UpdateManager.process_change(config, …)` (import it; it is already a
  method on a class that only needs `configs` and `idmap`). Do not
  duplicate the logic.
* `harvest_pages(my_slice, max_slice)` on the protocol (used by
  `harvest_parallel.sh --pages`). Keep it.
* `last_harvest` default `"0001-01-01…"` — the refactor changed it to
  `"0000-…"` and compares `[:4] == "0000"`. Keep `main`'s `0001` and compare
  against that.
* Backwards‑compatible class names: keep `Harvester`, `ASHarvester`,
  `PmhHarvester` importable — configs name `harvesterClass:
  "process.base.harvester.ASHarvester"` and
  `"…getty.harvester.GettyHarvester"`. The new driver class **is**
  `ASHarvester`; add `PmhHarvester = ASHarvester` with protocol default
  `PmhProtocol` chosen by `pmhEndpoint` in config; `GettyHarvester` becomes
  a driver subclass whose default protocol is `GettyProtocol`. `UpdateManager`
  on `main` calls `harvester.crawl()` and reads `harvester.last_harvest` /
  `.fetcher` — add thin pass‑throughs on the driver so `run-harvest.py`
  keeps working until item 33.

## Known bugs in the source — do not copy

* `ASHarvester.harvest_from_list` uses `self.harvester.fetcher` before
  `prepare()` created it when called standalone; create the protocol in
  `__init__`, not in `prepare`.
* `ASProtocol.crawl()`/`process_items()` call `self.manager.update_progress_bar(...)`
  and `self.manager.increment_progress_bar(...)`. Replace with a `progress`
  callable handed to the protocol by the driver (`protocol.progress = self.progress`,
  default `null_progress`), so a protocol used from a script reports nowhere
  and never dereferences a manager. `self.manager.log(...)` → `logger`.
* `count_lines()` duplicated from the loader; import from one place
  (`lux_pipeline/process/utils/io.py`, new tiny module) or leave it on the loader and reuse.

## Steps

1. Restructure `harvester.py`; keep every public name.
2. Update `getty/harvester.py` (`GettyHarvester2` on `main` is dead code
   — leave it but do not port anything into it).
3. `tests/test_harvester_protocols.py`: feed `ASProtocol` a fake session
   returning two canned pages (reverse‑ordered items, `prev` links, a
   `delete` and an `update`); assert the yielded tuples and that
   `last_harvest` stops the walk. Test `GettyProtocol` rewrites
   `data.getty.edu/vocab/aat/…` to the namespace.
4. `python run-harvest.py --ycba` dry check against a stub (or the live
   stream with `last_update` set to now).

## Acceptance criteria

* Existing configs load (`harvesterClass` names unchanged).
* `run-harvest.py` works unchanged.
* Tests pass.
