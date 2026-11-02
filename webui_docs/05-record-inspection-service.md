# W05. Record inspection service and `lux show`

**Size:** M · **Depends on:** — · **Unblocks:** W08, W12
**Read:** `lux_pipeline/cli/viz.py` (`typed_uri`, `replay`, `node_labels`,
`graph_edges`, `shortest_path`, `enable_debug`), `lux_pipeline/cli/interactive.py`,
`lux_pipeline/config.py` (`split_curie`, `split_uri`, `canonicalize`,
`make_qua`, `split_qua`, `is_qua`), `lux_pipeline/storage/cache/abstract.py`
(the whole contract), `lux_pipeline/storage/idmap/postgres.py`
(`get`, `get_cluster`, `_check_qua`), `lux_pipeline/storage/idmap/filesystem.py`,
`lux_pipeline/process/base/acquirer.py` (`acquire`, `__getitem__`),
`lux_pipeline/process/validator.py`, `manage-data.py` (`--debug-record`,
to see what it printed), `refactor_docs/42-build-roles.md` § "What `type`
controls today" (the key formats)

## Intent

One module, `lux_pipeline/records.py`, that answers "what is this, and
where is it?" for any reference an operator can type, by reading the caches
and the idmap through their public contracts; and a command, `lux show`,
that prints the answer:

```
lux show http://vocab.getty.edu/aat/300033618
lux show aat:300033618                       # curie
lux show ycba:obj/12345 --cache datacache    # one cache, full JSON
lux show 7f3a...-uuid                        # a YUID: the merged record and the cluster
lux show --sample aat --cache recordcache --type Type -n 20
lux show aat:300033618 --validate
```

## Files

| Action | Path | Notes |
|---|---|---|
| Create | `lux_pipeline/records.py` | the service |
| Create | `lux_pipeline/cli/show.py` | `lux show` (`BaseHandler`) |
| Edit | `lux_pipeline/cli/viz.py` | `replay`, `node_labels`, `graph_edges`, `shortest_path` are already functions; add `graph_data(cfgs, uri_a, uri_b) -> dict` that does what `process` does up to drawing, and have `process` call it. No change to what is drawn |
| Create | `tests/test_records.py`, `tests/test_cli_show.py` | |
| Edit | `CLAUDE.md` table, `docs/codebase.md` | |

## Design

```python
@dataclass
class Ref:
    kind: str            # "yuid" | "uri" | "curie" | "identifier"
    text: str            # what was typed
    source: dict | None  # the source cfg (internal/external) or None for a yuid
    group: str | None    # "internal" | "external"
    identifier: str | None
    uri: str | None      # canonical full URI (cfgs.canonicalize), None for a yuid
    rectype: str | None  # from ##qua if given
    yuid: str | None     # for kind yuid, or once resolved

def resolve(cfgs, text, source=None) -> Ref
```

`resolve` rules, in order: strip; a bare UUID (regex, 36 chars with
hyphens or 32 hex) or `cfgs.internal_uri + uuid` → `yuid`; contains
`##qua` → split with `cfgs.split_qua`, keep `rectype`, resolve the rest;
starts with `http` → `cfgs.split_uri` (ValueError "no configured source
matches" if None), `uri = cfgs.canonicalize(text)`; contains `:` and the
part before it is a source name (`cfgs.internal`/`external`/`results`, plus
`yuid`/`final`/`merged`/`ml` as `split_curie` accepts) → curie; otherwise
`identifier`, which needs `source` (ValueError naming the ambiguity if
not given). Resolution never fetches anything over the network.

```python
def cache_keys(cfgs, ref) -> dict        # {"datacache": [identifier], "recordcache": [identifier, identifier##quaType...], "merged": [yuid]}
```

the keys to try per cache, both forms for a record cache: bare for internal
sources, typed for external (`refactor_docs/42` § storage format); when
`rectype` is unknown and the source is external, try the bare key and then
every `cfgs.ok_record_types` type's typed key through `get_multi` (one
query on Postgres).

```python
def journey(cfgs, ref, *, mapped=True, idmap=None) -> dict
# {"ref": asdict(ref),
#  "datacache":   {"key", "row"} | None,         row = the cache row with "data"
#  "mapped":      {"record"} | None,              src["acquirer"].acquire(identifier, store=False) when mapped and the source has an acquirer; never stores
#  "recordcache": {"key", "row"} | None,
#  "reconciled":  {"key", "row"} | None,          external sources' reconciledRecordcache, if the cfg has one
#  "identity":    {"yuid", "members": [typed uris]} | None,   idmap.get_cluster(typed uri) or, for a Ref of kind yuid, idmap[yuid]
#  "merged":      {"key", "row"} | None,          cfgs.results["merged"]["recordcache"][yuid]
#  "marklogic":   {"key", "row"} | None,          cfgs.results["marklogic"]["recordcache"][yuid] if configured
#  "notes": [str]}                                e.g. "recordcache: found under the typed key", "no idmap", "mapped: acquirer raised X"
```

Every lookup is guarded: a store that raises adds a note and the entry is
`None`; `journey` never raises after `resolve` succeeded. Rows are returned
as the cache gives them (`insert_time` etc. as datetimes); the caller
serialises (`json.dumps(default=str)`).

```python
def sample(cache, n=50, typ=None, since=None) -> list[str]
```

`iter_keys_type(typ)` when `typ`, else `iter_keys_since(since)` when
`since`, else `iter_keys()`; `itertools.islice(..., n)`; then
`cache.end_read()` (a no-op on the filesystem cache, closes the read
transaction on Postgres). `typ="Concept"` is honoured by the contract.

```python
def metadata(cache, key) -> dict       # {field: value} for METADATA_FIELDS via cache.metadata(), None entries dropped
def validate(cfgs, rec) -> list[str]   # cfgs.validator.validate(rec["data"] if "data" in rec else rec); [] when valid; raises FileNotFoundError naming the missing schemas (validator.missing())
def identity_graph(cfgs, uri_a, uri_b) -> dict   # viz.graph_data
```

`viz.graph_data(cfgs, uri_a, uri_b)` returns
`{"nodes": [{"id": label, "uri", "source"}], "edges": [{"from", "to", "kind"}], "path": [labels] | None, "yuid": ...}`
— `process` then draws from it exactly as it does now.

### `lux show`

`BaseHandler`. Positional `ref` (optional with `--sample`), `--source`
(for a bare identifier; note the global `--source` exists: reuse
`args.source`), `--cache` (one of datacache / recordcache / reconciled /
merged / marklogic, or `all`, default a summary of all), `--validate`,
`--sample SOURCE`, `--type`, `--since`, `-n`, `--raw` (JSON only, for
piping). Summary output: one line per cache with found / not found, the
key used and the metadata; the identity cluster one member per line; then
the record JSON of the chosen cache (`--cache`) or of the first found.
Rich when a TTY (a JSON syntax-highlighted panel), plain otherwise.

## Known hazards

* **Never store.** `acquire(..., store=False)`; `enable_debug`'s
  `acquirer_should_store = False` for the graph. A look must not change a
  cache.
* **Do not open the idmap with a memory cache in a long-lived process.**
  `journey` takes `idmap=None` and calls `cfgs.get_idmap()` itself; the
  web (W08) passes its own handle with the memory cache disabled. The
  Postgres `get` returns a *set* for a `yuid:` key and a string otherwise.
* Result-group sources (`merged`, `marklogic`) have `acquirerClass: null`;
  `journey` for a yuid skips "mapped".
* `split_uri` returns `(source cfg, identifier)` after `fix_identifier`,
  which may change the identifier (`.html` stripped, mapper rules). Show
  the identifier the cache is keyed by.
* `iter_keys_type` on the filesystem cache reads every record; fine for
  the fixture, slow on a Postgres cache with no type index — `sample`
  documents that `typ` costs a scan and W08's page says so.
* A Postgres cache's iterators use a server-side cursor; forgetting
  `end_read()` holds a transaction open in the server for ever.

## Steps

1. `resolve` + tests (no stores needed: a `Config` with `instantiate=False`).
2. `cache_keys`, `sample`, `metadata`, `journey` + tests on the fixture's
   filesystem caches and idmap.
3. `validate` (test with the schemas missing: the error names them).
4. `viz.graph_data`; `identity_graph`.
5. `lux show`.

## Tests

On `tests/webui_fixtures.make_configs(tmp, instantiate=True)` with
`seed_records`:

* `resolve`: a full AAT URI → external aat, identifier `300033618`; the
  curie; `##quaType`; a UUID → yuid; a bare identifier without `--source`
  raises; an unknown URI raises naming it.
* `journey` for a seeded ycba record: datacache and recordcache found
  under the bare key, identity from the seeded idmap, merged found under
  its yuid; for a seeded aat record the recordcache hit is under the typed
  key and the notes say so; for a missing record every entry is `None`
  and nothing raised.
* `sample`: n honoured, `typ="Type"` filters, `since` filters; `end_read`
  is called (a cache subclass that records the call).
* `validate`: with no schema directory raises `FileNotFoundError` naming
  `core.json`.
* `graph_data` with `replay` patched to return a small graph: nodes,
  edges and path as specified; `lux viz --help` unchanged.
* `lux show` plain output contains the key lines; `--raw` is valid JSON.

## Acceptance criteria

* Tests pass; nothing under `lux_pipeline/process` or `storage` changed.
* `lux show aat:300033618` on the build machine (Rob) prints the record's
  caches, cluster and JSON; `lux viz` draws as before.
