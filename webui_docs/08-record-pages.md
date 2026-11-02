# W08. Record pages

**Size:** L · **Depends on:** W05, W06
**Read:** `05-record-inspection-service.md`; `lux_pipeline/records.py` (as
merged); `lux_pipeline/storage/cache/abstract.py` (`METADATA_FIELDS`,
`CONCEPT_TYPES`); `DESIGN.md` §3 (Records), §5

## Intent

Look anything up and see its journey through the pipeline; sample a cache.

| Method, path | What |
|---|---|
| `GET /records` | the search box, a source + cache browse panel, recent lookups (session, in memory) |
| `GET /records/lookup?q=...&source=...` | resolves and redirects to `/records/{kind}/{...}` or renders the error inline |
| `GET /records/r/{ref}` | the journey page for a URI / curie / typed key (`ref` URL-encoded) |
| `GET /records/y/{yuid}` | the journey page for a YUID |
| `GET /records/r/{ref}/{cache}` | one cache's row, full JSON, raw download (`?download=1`) |
| `POST /records/r/{ref}/validate` | validation result for the chosen cache's record |
| `GET /records/browse?source=aat&cache=recordcache&type=Type&since=...&n=50` | sampled keys, each a link |
| `GET /api/records/resolve`, `/api/records/journey`, `/api/records/sample`, `/api/records/validate` | JSON |

## Design

`api/records.py`:

```python
def resolve_data(state, q, source=None) -> dict          # asdict(records.resolve(...)); ValueError -> 400 with the message
def journey_data(state, q, source=None, mapped=False) -> dict
    # state.source(ref.source["name"]) first so the caches exist; idmap=state.idmap(); records.journey(...)
    # rows serialised: data kept as objects; times as ISO strings; plus "sizes": {cache: len(json.dumps(data))}
def sample_data(state, source, cache, typ=None, since=None, n=50) -> {"keys": [...], "cache": ..., "cost_note": "type filter scans the cache" if typ}
def validate_data(state, q, cache) -> {"valid": bool, "errors": [...]} ; FileNotFoundError -> 409 with the schema message and the `lux initialize` hint
```

`mapped` defaults to **False** on the page (a button "Map it now" fetches
`?mapped=1`): mapping runs the source's mapper, which for some sources
reaches other caches and takes seconds; make it opt-in.

Journey page layout: a header with the typed text, kind, source,
identifier, canonical URI, the typed key; a row of tabs, one per journey
entry (datacache · mapped · recordcache · reconciled · identity · merged ·
marklogic), each tab's label carrying a dot (found / missing) and
disabled when the source cannot have it (a yuid has no datacache). Each
found tab: a `kv` of the metadata (`insert_time`, `record_time`,
`refresh_time`, `valid`, `change`, key used, size), then the JSON as a
collapsible tree (`jsontree.js`: expand/collapse nodes, every `id` value
that `cfgs.split_uri` recognises rendered as a link to `/records/lookup?q=`,
`type` values highlighted) with a *Raw* toggle (`<pre>`) and *Download*.
The identity tab lists members as links, marks the one you came from,
shows the YUID with a link to `/records/y/`, and a *Graph with…* button
(W12) beside every other member. A *Validate* button per record tab.
Notes from `journey` appear under the header.

Browse panel: source select (grouped internal / external / results),
cache select (the caches the chosen source has: `datacache`,
`recordcache`, `reconciledRecordcache`, `recordcache2`), type select
(`cfgs.ok_record_types` keys plus `Concept`), since (datetime-local), n.
Results: the keys, each a link to the journey, and the cost note.

Links from elsewhere: W07's source table links each source to
`/records/browse?source=<name>`; the Dashboard's sources card too.

## Known hazards

* `state.source(name)` instantiates that source (opens its caches); on the
  build machine that is a Postgres pool per source, created once and kept.
  Fine. Never call `instantiate_all` from a request.
* The idmap handle is `state.idmap()` with the memory cache off; pass it
  to `journey` every time.
* `jsontree.js` must build DOM nodes from the parsed object, never
  `innerHTML` with values (records contain arbitrary strings).
* URL-encode refs with `urllib.parse.quote(ref, safe="")` in templates
  (`##qua` and `/` inside); FastAPI path converter `{ref:path}` for the
  slashes, and still decode `%23`.
* Big records (a merged Set with thousands of members) — the tree
  collapses arrays over 50 items by default and shows "… 2,345 more".
* A record cache row's `data` may itself be a `{"data":..., "identifier":...}`
  envelope on some stores (see `FsCache.set`'s unwrapping); render what
  the cache returns, the service already normalises.

## Steps

1. `api/records.py` + JSON routes + tests.
2. Search page and lookup redirect.
3. Journey page (JSON `<pre>` first), tabs, metadata.
4. `jsontree.js` with links; raw / download.
5. Browse panel.
6. Validate.
7. `docs/web.md` section.

## Tests (`tests/test_web_records.py`)

On the seeded fixture:

* `/api/records/resolve?q=aat:300033618` gives kind curie, source aat;
  `?q=nonsense` is 400 with a message; a bare identifier without source
  is 400.
* `/api/records/journey?q=<seeded ycba uri>` has datacache and recordcache
  rows, identity with the seeded yuid, merged found; a missing record has
  every entry null and 200.
* `/records/r/<encoded ref>` renders the tabs, marks found / missing, and
  contains the seeded record's `id`; `/records/y/<yuid>` renders the
  merged record.
* `/records/r/<ref>/datacache?download=1` is `application/json` and equals
  the row's data.
* `/api/records/sample?source=aat&cache=recordcache&type=Type&n=2` returns
  two keys and the cost note; the cache's `end_read` was called (spy).
* `POST /records/.../validate` without schemas is 409 and names
  `core.json`.
* The journey page's `<script>` contains no raw `</script>` from record
  data (use a record containing that string in the fixture).

## Acceptance criteria

* On the build machine (Rob): `lux serve`, look up an AAT URI, a YCBA
  curie and a YUID; each page renders in under a few seconds; the browse
  sample of a Postgres cache leaves no idle-in-transaction connection
  (`SELECT state FROM pg_stat_activity`).
* Tests pass.
