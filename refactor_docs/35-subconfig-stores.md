# 35. Sub‑config stores

**Priority:** P1 · **Size:** S · **Depends on:** 04 · **Unblocks:** the `pipeline-configs` repository
**Read from:** `rob_refactor:lux_pipeline/config.py` (`subconfig_stores`, `instantiate_config`, the `"type": "config"` branch of `handle_config_record`)

## Intent

Let a record in `config_cache` of `"type": "config"` point at another cache
whose records are *more* source configurations, so a deployment can keep
its private sources (or a shared public set) in a separate directory that is
loaded after the main one:

```json
{"name": "shared_sources", "type": "config", "datacacheClass": "storage.cache.filesystem.FsCache",
 "base_dir": "/srv/lux/shared-configs", "tabletype": "cache"}
```

## What to port

`handle_config_record`'s `"config"` branch, `instantiate_config()` (path
smushing of the `*_dir` keys relative to the parent config, `datacache`
creation, optional fetcher/harvester/loader/downloader for the store
itself), and the second loop in `__init__` that feeds each sub‑store's
records back through `handle_config_record`.

## What not to port

`storage/cache/github.py` (`GithubCache`) — the store class the refactor
used for this; `FsCache` over a git checkout gives the same result without
the API rate limit. Also the `instantiate_config` branch that overwrites
absolute `*_dir` values.

## Why required

The `pipeline-configs` repository (a sibling checkout; the live, reproducible
configuration from 2026‑10 on) keeps its sources, maps and MarkLogic stores
behind three `"type": "config"` records in `configs/system/config_cache/`
that point at `configs/subs/{sources,maps,mls}_cache/`. `main` logs those
records as an unknown type and skips them, so a `Config` built from that
repository has no sources at all until this item lands. Promoted from P3 on
2026‑10‑26 for that reason.

The sub‑store records carry a *relative* `base_dir`
(`pipeline-configs/configs/subs`) and the system record's `base_dir` is a
per‑machine placeholder; decide in this item whether a relative sub‑store
path resolves against the configuration directory (the config cache's own
`base_dir`) or the data `base_dir`, and document it in the repository's
README. `LUX_BASEPATH` points at `configs/system` so the bootstrap finds
`config_cache/`.

**Decision (landed with this item).** A relative `base_dir` resolves
against the directory the configuration was read from (`Config.config_dir`,
the `LUX_BASEPATH` directory), never the data `base_dir`; an absolute one is
left alone; a store directory that does not exist is an error. The
`pipeline-configs` records therefore need `"base_dir": "../subs"` in place
of `"pipeline-configs/configs/subs"` — the one edit the repository needs for
this item, applied when the repository is next touched. `tests/test_subconfig_stores.py`
loads a copy of the repository with that edit when the checkout is beside
this one.

**Live config copies.** `config_cache/config_cache/` is an untracked copy of the
build machine's configuration files (see README ground rule 5). In the same PR,
apply this item's config changes there: any `"type": "config"` record the live instance is to use (none, until the build machine switches to the `pipeline-configs` checkout). Check the edited files still
parse as JSON and that every `*Class` value resolves through `importObject`
without a legacy warning, and list the edited files in the PR description.
Leave the local instance alone.
