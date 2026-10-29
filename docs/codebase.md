
## Codebase Notes

Ordered from Easiest to Most Diabolical to understand

### Code Layout

Data source specific code is in lux_pipeline/sources/, one directory per source (plus yale/ for Yale units and lux/ for LUX's own outputs).

The rest of the code is divided according to function:

* config.py: read the configuration records and build the components they name
* runtime.py, log.py, progress.py: the per-process configuration, logging, and the progress protocol (below)
* cli/: the `lux` command, one module per command
* process/: the work itself
  * base/: the base classes the sources extend (acquirer, downloader, fetcher, harvester, index_loader, loader, mapper, reconciler)
  * collector: follow links in records to collect more records
  * reconciler: given a record, find other records that describe the same entity
  * identity_resolver: resolve the identity map from the assertions reconcile logs
  * merger, merge_batch: merge records that describe the same entity down to one record
  * reidentifier: rewrite external URIs in a record to internal identifiers
  * reference_manager: manage the references between records
  * validator: validate the JSON structure of a record
  * phases/, tasks/: the build phases and the managers that run them (below)
* storage/: caches (Postgres, filesystem; `cache/abstract.py` is the interface they implement), the identity map (Postgres, Redis, in-memory), MarkLogic, and backup / restore

The sources each contain:

* fetcher: retrieve a record over the network
* index_loader: load an index for reconcilers to work with from local data
* loader: load a cache with records from a dataset dump file
* mapper: transform from raw data into linked art
* reconciler: dataset specific reconciliation details, called from higher level reconciler

There is also at the sources level:

* mapper_utils: utility functions that mappers use


### Harvester

* Purpose: Consume activity streams data, which references records, and download the records into the appropriate data cache
* Background: Uses IIIF Change Discovery standard https://iiif.io/api/discovery/1.0/ 
* Complexity: Relatively self contained code and not too complicated to follow, but does rely on external data
* Entry Point: crawl() is the place to start. It then sets in motion the harvest across multiple collections and pages

For each ActivityStreams collection in the configuration, walk backwards through all of its pages, and retrieve all of the records referenced. This process stops when either it runs out of pages, or it hits the timestamp at which it was last run (fetched from the cache by set_last_harvest()). 

fetch_collection() merely gets the collection JSON and records the reference to the last page.
Then while there are pages, fetch_page() fetches the page JSON and extracts the list of items. It returns them to be fed to process_items() which does most of the work.
process_items() steps through the list of items and determines whether to stop or process the item, mostly consisting of workarounds for poorly constructed feeds. The actual work is at the end of the function where it fetches the record, gets its identifier, and adds it to the data cache in self.storage


### Reconciler

* Purpose: Given a record, find other records that describe the same entity and update the list of equivalents on the input
* Background: 
* Complexity: Wrapper around the reconcilers in sources, so very short
* Entry Point: reconcile() is the only function

reconcile() is given a record, and a flag for what sort(s) of reconciliation to attempt. There are currently two flags: 'name' (exact match on primary name in the target dataset) and 'uri' (the current record's URI is matched against equivalents in the target dataset).
The function continues to attempt to find new equivalents iteratively until no new equivalents are found. This means that on the first iteration, it might find a new URI based on name and then the second round might use the URI found in the first iteration to find another URI in a different dataset.  It finds the URIs by calling reconcile() on each of the configured source datasets that have reconcilers, and then adds them to the `equivalent` property on the given record.

#### Source Reconcilers

* Purpose: Given a record, find other records in the source dataset that describe the same entity
* Background: Called via Reconciler above
* Complexity: Currently quite low, but could be more sophisticated in the future
* Entry Point: reconcile()

Start with sources/reconciler.py and look at SqliteReconciler. 

The SQLite databases interact like dictionaries. The keys are either external URIs (e.g. an LC uri in the wikidata identifier index) or lowercase, strip()ed labels. The values are the identifier of the source's record that matches, and its class. 

The reconcile() function takes a record and the type of reconciliation to attempt. For either identifier or name it finds the value from the record (primary name in identified_by or id in equivalent) and then looks in the SQLite index for that value. It returns None if there are no matches, or the identifier if there is one.

The should_reconcile() function determines if it is useful to try and reconcile at all. For example, we do not want to try and reconcile a physical object against data that doesn't have physical objects.

In the future, source reconcilers might be significantly more clever... but they would still need to run against local data, as they are called MANY times.




### Base configuration keys

`base.json` (or `system.json`, which is read in preference when present) holds the
machine-specific settings. Besides the `*_dir` paths, which are made absolute against
`base_dir` when relative, two optional keys have defaults:

* `max_workers`: how many workers a parallel phase starts by default. Defaults to three
  fewer than the machine's cores (Postgres, Redis and the controlling process need some),
  never fewer than one.
* `temp_dir`: scratch space for sorts and partial downloads. Defaults to `tmp` under
  `base_dir`.

Per-kind defaults for stores can be given as `{"type": "default", "name": "cache" | "map"
| "marklogic", ...}` records; keys in them are merged under every store config of that
kind that does not set them itself. An instance without them keeps using the single
`caches.json` record for its caches.


### runtime.py, log.py, progress.py

* `runtime.get_configs()` builds the process's `Config` once (from `LUX_BASEPATH`, read from the nearest `.env`), and `configure_logging()` sets up the shared `lux_pipeline` logger. Every worker process builds its own; connections are never shared between processes.
* Library code logs through `logging.getLogger("lux_pipeline")` and never prints.
* Components report progress through a `progress=` callable (`progress.null_progress` by default). A worker gets a `StatusWriter`, which rewrites `<log_dir>/<command>-<timestamp>-w<n>.status`; the worker's log goes to the matching `.log`. `lux watch` and the console view read those files, so no component knows whether, or how, it is being displayed.


### cli/

* `entry.py` is the `lux` script: it parses the global options (`--source`, `--max-workers`, `--my-worker`, `--engine`, `--no-ui`, `--prefix`, ...), finds the command module (`lux foo-bar` is `cli/foo_bar.py`; a dotted name is an extension module), loads the configuration and calls the module's `CommandHandler.process`. The exit status is non-zero on any failure.
* `_handler.py`: `BaseHandler` for commands that do one thing in this process (`counts`, `clear`, `identify`, ...), and `CommandHandler` for the parallel phases, which resolves the sources, builds a manager from `process/tasks/` and runs it on an engine with the console view (`_view.py`, the only user of Rich) watching.
* `help.py` lists every module with a `CommandHandler`, using the first line of its docstring.


### process/tasks/

* `_task_manager.py`: `TaskManager`, the base of every manager, and the engines. A manager's `_distributed(n, cfgs, progress)` is what worker `n` of `max_workers` does; the engine decides where it runs:
  * `mp`: spawned processes (the default)
  * `null`: this process; with `--my-worker i` it is slice `i` of `--max-workers`, for shell loops that start every slice under `nohup` (`run-all2.sh`, `*_parallel2.sh`)
  * `ray`: Ray tasks
* One manager per parallel command: `download_task`, `load_task`, `index_load_task`, `harvest_task`, `reconcile_task`, `merge_task`, `export_task`. Every record is processed by exactly one worker: sources are split by worker, or records by `seen % max_workers == n`.
* A manager may do work in the driver before or after its workers: `ReconcileManager` runs identify, metatypes and write-refs once every worker has finished; `MergeManager` checks the update token and `reference_uris.txt` before any worker starts.


### process/phases/

The bodies of the build phases, moved out of the old `run-*.py` scripts unchanged so that `lux` and the legacy scripts run the same code: `reconcile.reconcile_slice`, `identify.resolve_identity`, `merge.merge_slice`, `export.export_slice` (and `export_nt_slice`, `export_sources_slice`, `export_external`). Each takes the configuration, the slice (`my_slice`, `max_slice`) and a `progress=` callable; reconcile and merge also take the sources chosen by their `select_sources`. The loops inside them are timed by `process/timing.PhaseTimer`.


### The loader's input_files grammar

A source config describes its dump files as `input_files`: a dict of load type (`records` by default) to a list of `{"path"|"url", "type"}` entries, where `type` is a small path language (from `process/base/loader.py`; `lux guess-dump-type` writes it for you):

```
    (container[.compression]/)*format[.compression]

    containers:  dir dirs pair zip tar lines dict array arraylines xmls lmdb
    formats:     json raw other
    compression: gz bz2 z

    dir         a directory of files
    dirs, pair  a directory hierarchy of files (pair: two-character subdirs)
    zip, tar    an archive of members
    lines       a file where each line is a member
    dict        a JSON file whose top level is an object; each value is a member
    array       a JSON file whose top level is an array; each item is a member
    arraylines  a JSON array written one record per line
    xmls        an XML file; each child of the root is a member
    lmdb        an LMDB distribution store (make_*_lmdb.py); each value is a member
    json        a JSON record (a file, a member, or an already parsed item)
    raw         an already parsed JSON item (from dict / array)
    other       anything else, kept as text under {"data": ...}
```

For example `tar.gz/lines.bz2/json` is a gzipped tar whose members are bzip2'd JSON Lines files; a datacache export is `zip.bz2/json`; an LMDB store of zlib-compressed records is `lmdb/json.z`. Slicing happens at the format step, so N workers can read the same file; every worker must see the children in the same order, which is why directory listings are sorted. Sources that still override `load()` keep the legacy path.
