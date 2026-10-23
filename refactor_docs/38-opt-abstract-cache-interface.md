# 38. Optional: `AbstractCache` interface class

**Priority:** P3 · **Size:** S · **Depends on:** —
**Read from:** `rob_claude_refactor:lux_pipeline/storage/cache/abstract.py` (≈250 lines; the cleaned‑up version)

## Intent

Add `lux_pipeline/storage/cache/abstract.py` as the documented interface every
cache backend implements (`len`, `latest`, `len_estimate`, `metadata`,
`set_metadata`, the `iter_*` family, `has_item`, `get`, `set`, `delete`,
`clear`, bulk writes, and the dict‑style dunder methods), and make
`PooledCache` and `FsCache` inherit from it.

## Why optional

`main`'s `PooledCache` already implements a superset (`get_multi`,
`has_multi`, `get_fresh`, `defer_commits`, `checkpoint`, `iter_keys_type_slice`,
maintenance). The abstract class adds documentation and a place for the
shared `__setitem__` logic, not behaviour. Worth doing if a third backend
(the `LmdbCache` sketch on `rob_claude_refactor`, `storage/cache/lmdb.py`)
is ever finished.

## If done

* Add `main`'s extra methods to the interface with docstrings and
  `raise NotImplementedError()` defaults.
* Do not port `LmdbCache` from `rob_claude_refactor`: it is a stub
  (`env` undefined in `__init__`, every iterator `NotImplementedError`).
* `_prepare_write_params` from the refactor is a good shared helper for
  `set()`/`set_bulk()`; adopt it only if `PooledCache.set` can call it
  without changing its SQL.
