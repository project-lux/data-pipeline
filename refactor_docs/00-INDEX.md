# Index — work top to bottom

Priority: **P0** foundations (everything else needs them) · **P1** the
features the refactor exists for · **P2** completes the CLI and retires the
old scripts · **P3** optional, do last or never.

Size is the rough amount of new/changed code: S < 150 lines, M < 500, L more.

Tick the box in the PR that lands the item. `CLAUDE-REFACTOR-FIXES.md` maps
every `rob_claude_refactor` fix to the item that carries it; check it when
you finish an item.

| # | Document | Pri | Size | Depends on | Done |
|---|---|---|---|---|---|
| 00 | [Layout: rename to `lux_pipeline`, flatten `sources/` (two PRs)](00-layout-rename-and-flatten.md) | P0 | L (mechanical) | — | ☑ |
| 01 | [Logging in core modules](01-logging-core.md) | P0 | M | 00 | ☑ |
| 02 | [Progress protocol (`lux_pipeline/progress.py`)](02-progress-protocol.md) | P0 | S | 00 | ☑ |
| 03 | [Runtime context (`lux_pipeline/runtime.py`)](03-runtime-context.md) | P0 | S | 00 | ☑ |
| 04 | [Config: system/defaults/paths/downloader](04-config-system-defaults.md) | P0 | M | 01 | ☑ |
| 05 | [Packaging, `lux` entry point, handler base](05-packaging-cli-entry.md) | P0 | M | 03, 04 | ☑ |
| 06 | [Console UI: `BuildView` and `lux watch`](06-rich-tui.md) | P0 | M | 01, 02 | ☑ |
| 07 | [Task engines and `TaskManager`](07-task-engines.md) | P0 | M | 02, 03, 05, 06 | ☑ |
| 08 | [`lux counts`](08-counts-command.md) | P1 | S | 05 | ☑ |
| 09 | [`lux clear`](09-clear-command.md) | P1 | S | 05 | ☑ |
| 10 | [`lux list`](10-list-command.md) | P1 | S | 05 | ☑ |
| 11 | [Mapper hooks (`make_export_filename`, `extract_index_data`)](11-mapper-hooks.md) | P1 | S | 01 | ☑ |
| 12 | [Loader micro‑DSL (`process/base/loader.py`) + tests](12-loader-dsl.md) | P1 | L | 02, 04, 11 | ☑ |
| 13 | [`LoadManager` and `lux load`](13-load-command.md) | P1 | S | 07, 12 | ☑ |
| 14 | [Downloader base class](14-downloader-base.md) | P1 | S | 02, 04 | ☑ |
| 15 | [`DownloadManager` and `lux download`](15-download-command.md) | P1 | S | 07, 14 | ☐ |
| 16 | [Harvester: protocol / driver split](16-harvester-protocols.md) | P1 | M | 02, 04 | ☑ |
| 17 | [`HarvestManager` and `lux harvest`](17-harvest-command.md) | P1 | M | 07, 16 | ☐ |
| 18 | [Index loader as a managed task](18-index-loader.md) | P1 | M | 02 | ☐ |
| 19 | [`IndexLoadManager` and `lux load-index`](19-load-index-command.md) | P1 | S | 07, 18 | ☐ |
| 20 | [Reconcile phase as a module](20-reconcile-phase-module.md) | P1 | M | 01 | ☐ |
| 21 | [`ReconcileManager` and `lux reconcile`](21-reconcile-command.md) | P1 | M | 07, 20 | ☐ |
| 22 | [`lux identify`, `lux new-token`, `lux write-refs`](22-identify-and-token-commands.md) | P1 | S | 05, 20 | ☐ |
| 23 | [Merge phase as a module](23-merge-phase-module.md) | P1 | M | 01 | ☐ |
| 24 | [`MergeManager` and `lux merge`](24-merge-command.md) | P1 | M | 07, 23 | ☐ |
| 25 | [Export phase as a module](25-export-phase-module.md) | P1 | M | 01 | ☐ |
| 26 | [`ExportManager` and `lux export`](26-export-command.md) | P1 | M | 07, 25 | ☐ |
| 27 | [`lux interactive`](27-interactive-command.md) | P2 | S | 05 | ☐ |
| 28 | [`lux initialize` and `lux testinstall`](28-initialize-testinstall-commands.md) | P2 | S | 05 | ☐ |
| 29 | [Source loaders moved onto the DSL](29-source-loaders-dsl.md) | P2 | M | 12, 13 | ☐ |
| 30 | [Source downloaders](30-source-downloaders.md) | P2 | S | 14, 15 | ☐ |
| 31 | [Shell orchestration on the null engine](31-shell-scripts-null-engine.md) | P2 | M | 06, 13, 17, 21, 22, 24, 26 | ☐ |
| 32 | [Docs: install, README, rebuild](32-docs-install.md) | P2 | S | 31 | ☐ |
| 33 | [Retire the legacy scripts](33-legacy-scripts-retirement.md) | P2 | S | 31, 32 | ☐ |
| 39 | [`lux backup` and `lux restore`](39-backup-restore-commands.md) | P2 | L | 02, 05, 08, 09 | ☐ |
| 40 | [`lux guess-dump-type`](40-guess-dump-type-command.md) | P2 | M | 05, 12 | ☐ |
| 34 | [Optional: named `indexes` config block](34-opt-named-indexes-config.md) | P3 | M | 18, 19 | ☐ |
| 35 | [Optional: sub‑config stores](35-opt-subconfig-stores.md) | P3 | S | 04 | ☐ |
| 36 | [Optional: `lux viz`](36-opt-viz-command.md) | P3 | S | 05 | ☐ |
| 37 | [Optional: logging sweep over `lux_pipeline/sources`](37-opt-source-logging-sweep.md) | P3 | L | 01 | ☐ |
| 38 | [Optional: `AbstractCache` interface doc class](38-opt-abstract-cache-interface.md) | P3 | S | — | ☐ |

## Dependency picture

```
00 rename + flatten ──► everything below

01 logging ─┬─► 04 config ─► 05 cli entry ─┬─► 08 counts   09 clear   10 list
            │                              │
02 progress ┼─► 06 BuildView / lux watch ──┤
            │                              ▼
03 runtime ─┴──────────────────────────► 07 engines ──┬─► 13 load      (needs 12 loader DSL  ◄─ 11 mapper hooks)
                                                      ├─► 15 download  (needs 14 downloader)
                                                      ├─► 17 harvest   (needs 16 harvester split)
                                                      ├─► 19 load-index(needs 18 index loader)
                                                      ├─► 21 reconcile (needs 20 phase module) ─► 22 identify/token
                                                      ├─► 24 merge     (needs 23 phase module)
                                                      └─► 26 export    (needs 25 phase module)
                                                                 │
27 interactive, 28 initialize/testinstall (need 05)              ▼
39 backup/restore (needs 05, 08, 09) ─► 33 retires backup_tables.sh
40 guess-dump-type (needs 05, 12) ─► 29 drafts the input_files blocks
29 source loaders (needs 13), 30 source downloaders (needs 15) ─► 31 shell scripts ─► 32 docs ─► 33 retire scripts
```

Items at the same depth are independent of each other and can be done in
parallel by different people, e.g. 08/09/10, or 12/14/16/18/20/23/25.
