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

## Decisions (as done)

Rob: rather than leave `FsCache` short of the interface, fix it so the
abstract class is the API contract every cache honours.

* `storage/cache/abstract.py` is written for `main`, not copied: the
  refactor's version raises `ValueError` on a bad yuid key where `main`'s
  `PooledCache` logs and returns None, and stubs everything.
* Methods a backend must provide raise `NotImplementedError` (get, set,
  delete, has_item, clear, len, latest, metadata, set_metadata, iter_keys,
  iter_records, the `_slice` and `_since` iterators). The rest have working
  defaults built on those, so a backend that has the required ones has the
  whole contract: `get_multi`, `has_multi`, `get_fresh`, `len_estimate`,
  `iter_keys_slice_mem`, the `iter_*_type` family, `iter_items`, bulk
  writes as `set()`, and the commit hooks as no-ops. A test checks both
  backends override every required method.
* Capabilities only some backends have stay out of it, because callers find
  them with `getattr` and a stub would make every cache appear to have them:
  native `backup` / `restore` / `backup_size`, Postgres maintenance
  (`optimize`, `bloat`, `rewrite`, ...), `get_like`. The module docstring
  lists them.
* `PooledCache` inherits it and overrides everything it had; its public
  attributes are unchanged. Its dict-style methods moved to the base class
  verbatim. `set()` uses `_prepare_write_params` (same checks, same row);
  `set_bulk()` does not, since it does no validation today and would start
  raising.
* `FsCache` now honours the whole contract:
  * the file's modification time is insert_time and record_time (rows used
    to carry `''`); `set(record_time=)` and `set_metadata("record_time")`
    move it; `latest()`, `has_item(timestamp=)` and the `_since` iterators
    use it (`iter_keys_since` used to ignore its timestamp);
  * `refresh_time`, `valid` and `change` go in `<key>.json.meta` beside the
    record, written only when one is set, so a portable restore of a
    Postgres backup keeps them and `post-build-portal.py`'s
    `set_metadata("change")` works; `delete()` and `clear()` remove them;
  * `iter_keys_slice`, `metadata`, `set_metadata`, `latest` are new; every
    method takes `_key_type`;
  * a missing record is logged at `debug`, not `warning`, as Postgres
    says nothing.
* `RedisDataCache` is left alone: nothing configures it.
