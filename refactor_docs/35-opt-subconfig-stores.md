# 35. Optional: sub‑config stores

**Priority:** P3 · **Size:** S · **Depends on:** 04
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

## Why optional

The live instance is a single directory. Useful when the Yale‑specific and
the public source configs are split into two repositories.
