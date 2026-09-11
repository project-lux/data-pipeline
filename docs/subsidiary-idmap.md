# A subsidiary identity map

`storage.idmap.subsidiary.IdMap` is an identity map that starts as a copy of
another one and diverges from it. The master is read and never written; the
subsidiary owns its own postgres tables; and what the two disagree about is a
query, not a guess.

The case it exists for is a reconciliation you are not ready to ship. You
want production's identities as the starting point — the same YUIDs, so a
record that moves can be compared against the one that shipped — but every
decision taken on top of them has to land somewhere production cannot see,
and afterwards you have to be able to say exactly what changed.

## Configuration

`docs/sample_config/map_idmap_sub.json`:

```json
{
	"name": "idmap_sub",
	"type": "map",
	"storeClass": "storage.idmap.subsidiary.IdMap",

	"masterMap": "idmap",

	"tableName": "idmap_sub",
	"memoryCacheSize": 200000,
	"copyUpdateToken": true,
	"logChanges": true,

	"prefix_map_out": {"yuid":"https://lux.collections.yale.edu/data/"}
}
```

| key | |
| --- | --- |
| `masterMap` | **Required.** The `name` of another map store config, resolved through `instantiate_map()` — so the master is whatever *its* config says it is. The sample `map_idmap.json` is redis; nothing here cares. |
| `tableName` | **Required**, and deliberately has no default. The parent class falls back to `idmap`, and a subsidiary that inherited that against a postgres master in the same database would write straight into the master's tables. |
| `copyUpdateToken` | Copy the master's update token onto a YUID as it is copied. Default true. Free against a redis master, which returns the token as a set member; one extra call per class against a postgres one. |
| `logChanges` | Write the `{table}_changes` audit trail. Default true. The origin snapshot is kept either way. |

Everything `storage.idmap.postgres.IdMap` takes also applies —
`pgHost`/`pgPort`/`pgUser`/`pgDbname` to point the subsidiary at a different
server from the record caches, `memoryCacheSize`, `asyncCommit`. With none of
them it uses the same postgres as the caches config, which is where the
master's tables would be too if the master is also postgres; the tables are
kept apart by `tableName`, and instantiation refuses to start if they are
not.

To run a phase of the pipeline against the subsidiary, point `idmap_name`
(`base.json`) at it. To use both in one process, ask for them by name:

```python
sub = cfgs.instantiate_map("idmap_sub")["store"]
```

## What it does

Nothing, until something asks. A key nobody has looked up does not exist in
the subsidiary. The first lookup that touches one copies what the master
knows into the local tables; from then on the local tables are the only thing
answered from, and the master is never consulted about that key again.

Three tables beyond the parent's `{table}` and `{table}_yuid`:

- `{table}_origin` — for every key ever resolved against the master, what the
  master said at the time. `yuid IS NULL` is a *recorded miss*, not an absent
  row.
- `{table}_changes` — append-only, a row per write, with what the key pointed
  at before and after.

The copy pulls the whole equivalence class, not the row asked for, and that
is load-bearing rather than an optimisation. Membership in this schema is
derived — a YUID's members are the rows carrying it — so a half-copied class
would answer `idmap[yuid]` with whichever members happened to have been
touched, and merging such a class locally would move those and leave the rest
resolving to the old YUID out of the master. Copying it whole means every
class this map has an opinion about is complete and local, and the reverse
direction cannot disagree with the forward one.

## Why the master is safe

"Changes must not affect the master" is the requirement the class exists to
satisfy, so it is enforced in three places rather than left to review:

1. The master store is wrapped in `ReadOnlyMaster`, which allows an explicit
   list of read calls and raises `ReadOnlyMasterError` on anything else.
   `set`, `mint`, `assign_bulk`, `clear`, `__setitem__` — all of them raise.
2. `tableName` is mandatory, and `_guard_distinct()` refuses to instantiate
   if the subsidiary's tables resolve to the master's on the same database.
3. Nothing copied in is ever copied over a local decision: every hydrating
   insert is `ON CONFLICT DO NOTHING`. A row already present carries a
   decision made here, and the master does not get to overwrite it.

The master is reached only through its public interface —
`get`/`get_multi`/`get_cluster`/`has_item`/`has_update_token` — which is why
a redis master, a postgres one and the in-memory one all work without the
subsidiary knowing which it has. Two differences between them are real and
handled:

- `get_cluster()` exists only on the postgres backend. `_master_cluster()`
  probes for it and otherwise falls back to the two lookups it replaces.
- redis keeps update tokens as pseudo-members of the YUID set; postgres keeps
  them in a column. Tokens are filtered out of incoming member sets, the same
  test `migrate-idmap.py` uses.

## Changing identities

`set(uri, yuid)` merges, exactly as it does in the parent: every member of
the URI's class moves onto the target. Both classes are copied in first, for
the reason above.

`remint(uri, slug)` gives one URI a brand-new YUID of its own and leaves the
rest of its old class where it is — the subsidiary's way of saying "this URI
is not the thing the master thought it was".

`mint(uri, slug)` keeps the parent's semantics, which are worth being
explicit about: it *adopts* an existing row rather than replacing it, and
after copying, the master's identity is an existing row. So minting for a URI
the master already knows returns the master's YUID and changes nothing. Use
`remint()` when a new one is the point.

`assign_bulk()` works, and is what an identity phase run against the
subsidiary would use. It copies the master's baseline for every member and
every YUID first, in batches — that is the expensive part of doing this at
scale, and it is the price of being able to say what changed. It is paid once
per key for the life of the map.

## Tracking what changed

Two things, deliberately:

**The divergence** is derived, not recorded: the live rows outer-joined to
what the master said about the same keys. It is correct whatever happened to
the audit trail, and it collapses a key that moved five times into the one
fact that matters — where it started and where it is now.

```
python idmap-subsidiary.py                          # summary
python idmap-subsidiary.py --diff                   # every divergence, TSV
python idmap-subsidiary.py --diff --kind swapped
python idmap-subsidiary.py --diff -o diverged.tsv
```

```python
for d in sub.iter_divergence():
    d["uri"], d["master_yuid"], d["local_yuid"], d["kind"]
```

`kind` is which side is missing: `swapped` (both have a YUID and they
differ), `added` (the master had nothing), `removed` (the mapping is gone
here), `unseen` (no origin row at all, so the master was never asked — only
reachable through `_import_state()` or hand-written SQL, and reported rather
than hidden because it is the one case where the baseline is unknown).

**The audit trail** is the history the snapshot cannot hold: order,
timestamps, intermediate states.

```
python idmap-subsidiary.py --changes --since 2026-09-01
python idmap-subsidiary.py --changes --op merge --op remint
```

Operations are `mint`, `remint`, `merge` (and `moved`, one per other member
the merge carried), `assign`, `delete`, `delete_yuid`, `add`, `detach`. It is
written outside the transaction that made the change and a failure to log
never fails a write, so treat the snapshot as authoritative and the trail as
the narrative.

There is deliberately no way to push a divergence back into the master. Read
`--diff` and apply it on purpose, with whatever review that deserves.

## Maintenance

`optimize()` vacuums all four tables, and `manage-data.py --vacuum` does not
know about this map — call it from `idmap-subsidiary.py --vacuum` or
directly. The same argument as the parent's applies: `assign_bulk()` leaves a
dead tuple per member it moves, and autovacuum's 20% scale factor will not
fire on a large map.

`clear()` truncates all four, the origin snapshot included — so the next
lookup starts the copy again from whatever the master says *now*.
