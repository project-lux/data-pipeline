# Data Transformation Pipeline Code

## Installation

See [docs/install.md](docs/install.md): system packages, `pip install -e .`,
`lux initialize`, `lux testinstall`, downloading and loading data.
[docs/rebuild.md](docs/rebuild.md) describes a rebuild step by step and
[docs/codebase.md](docs/codebase.md) is a tour of the code.

## Running

Everything runs through the `lux` command (`lux help` lists them all):

```
lux download --source all --type records --max-workers 10   # fetch the sources' dump files
lux load --source wikidata --max-workers 24                 # load a dump into its datacache
lux reconcile --source all --max-workers 24                 # reconcile, then identify, metatypes, write-refs
lux merge --source all --max-workers 24                     # one merged record per entity
lux export --max-workers 24                                 # JSONL for MarkLogic
```

`./run-all2.sh --all` runs a whole build. `lux counts` shows what is in the
caches, maps and indexes, and `lux watch --latest` attaches to a running
build from another terminal: every worker writes
`<log_dir>/<command>-<timestamp>-w<n>.status` and `.log` files, which is
what `lux watch` reads. The legacy `run-*.py`, `manage-data.py`,
`run-all.sh` and `*_parallel.sh` scripts still work until they are retired.

The ongoing port of the `rob_refactor` branch is planned and tracked in
`refactor_docs/` (start with its `README.md` and `00-INDEX.md`); `CLAUDE.md`
describes the working conventions.

## Architecture

![architecture diagram](docs/architecture2.png)


## Pipeline Components

### Future ITS Owned Components:
* Harvester: ActivityStreams based record harvester that stores to a Cache.
* Cache:  Record cache for storing local copies of JSON data. Currently postgres or filesystem.
* IdMap: Identifier Map that mints, manages and retrieves external/internal identifier sets. Currently Postgres, Redis or in-memory.

### Pipeline Components:

* config: Configuration of the pipeline, as JSON records in a cache.
* cli/: the `lux` command: one module per command (`lux help` lists them), the console view and `lux watch`.
* process/tasks/: the managers that run a phase's workers on an engine (`mp`, `null`, `ray`), one per parallel command.
* process/phases/: the body of each build phase (reconcile, identify, merge, export), shared by `lux` and the legacy scripts.
* process/Collector: Recursively collects identifiers for a given record.
* process/Reconciler: Reconcile records
* process/Merger: Merges two Linked Art records representing the same entity together.
* process/Reidentifier: Recursively external rewrite URIs in a record to internal identifiers, given an IdMap.
* process/ReferenceManager: Manages inter-record connections
* process/UpdateManager: Manages harvesting and updates to caches
* sources/\*/: one directory per source (plus yale/ for the Yale units and lux/ for LUX's own outputs), each holding the classes below.
* sources/\*/Acquirer: Wraps Fetcher and Mapper to acquire a record either from the network or a cache
* sources/\*/Fetcher: Fetches identified record from external source to a Cache.
* sources/\*/Harvester: Retrieve multiple records via IIIF Change Discovery or OAI-PMH
* sources/\*/Mapper: Maps from source format into Linked Art, or from Linked Art to discovery layer format (Marklogic, QLever)
* sources/\*/Reconciler: Determine if the entity in the given record is described in the external source.
* sources/\*/Loader: Load a dump of the data into the data cache.
* sources/\*/IndexLoader: Create an inverted index to reconcile records against this dataset.
* storage/MarkLogic: Marklogic storage of data
* storage/Cache: Data caches (Postgresql, file system)
* storage/Idmap: Key/value store API (Redis, file system, in-memory)


## External Sources: Implementation Status

| Source          | Fetch | Map | Name Reconcile | Load | IdxLoad |
| --------------- | ----- | --- | -------------- | ---- | ------- |
| AAT             |   ✅  |  ✅ |    ✅         | N/A | -       | 
| DNB             |   ✅  |  ✅ |     -         |  ✅ | -       | 
| FAST            |   ✅  |  -  |     -          |  -   | -      | 
| Geonames        |   ✅  |  ✅ |     -          | ✅  | -      | 
| LCNAF           |   ✅  |  ✅ |     ✅        |  ✅ | -       | 
| LCSH            |   ✅  |  ✅ |     ✅        |  ✅ | ✅      | 
| TGN             |   ✅  |  ✅ |     -         | N/A  | -       | 
| ULAN            |   ✅  |  ✅ |     ✅        | N/A  | -       | 
| VIAF            |   ✅  |  ✅ |     -         |  ✅  | -       | 
| Who's on First  |   ✅  |  ✅ |     -         | N/A  | -       | 
| Wikidata        |   ✅  |  ✅ |     -         |  ✅  | ✅     | 
| Japan NL        |   ✅  |  ✅ |     -         | N/A  | -       |
| BNF             |   ✅  |  ✅ |     -         | N/A  | -       |
| GBIF            |   ✅  |  ✅ |     -         | N/A  | -       |
| ORCID           |   ✅  |  ✅ |     -         | N/A  | -       |
| ROR             |   ✅  |  ✅ |     -         | N/A  | -       |
| Wikimedia API   |   ✅  |  ✅ |     -         | N/A  | -       |
| DNB             |   ✅  |  ✅ |     -         | N/A  | -       |
| BNE             |   ✅  |  ✅ |     -         | N/A  | -       |
| Nomenclature    |   -   |  -  |     -          | -    | -      |
| Getty Museum    |   -   |  -  |     -          | -    | -      |
| Homosaurus      |   -   |  -  |     -          | -    | -      |
| Nomisma         |   -   |  -  |     -          | -    | -      |
| SNAC            |   -   |  -  |     -          | -    | -      |


✅ = Done ; - = Not started ; N/A = Can't/Won't be done

* AAT, TGN, ULAN: Dump files are NTriples based. More effort to reconstruct than it would be worth. Instead we can use IIIF Change Discovery to synchronize.
* WOF: Dump file is a 33Gb sqlite db... we just use it as the cache directly
* FAST: Not implemented yet (needs to process MARC/XML)

### Fetching external source dump files

`lux download --source <name>` fetches the files a source's configuration
names (`remoteDumpFile` and `dumpFilePath`, or its `input_files` block)
into place; `--source all --max-workers 10` fetches every source's, and
`--no-overwrite` keeps what is already there. Wikidata's is huge: run it
under `nohup` (or in `tmux`) and get on with life in the mean time.
