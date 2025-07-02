# Data Transformation Pipeline Code

## Architecture

![architecture diagram](docs/architecture2.png)


## Pipeline Components:

* config: Configuration of the pipeline, as JSON records in a cache.
* process/Collector: Recursively collects identifiers for a given record.
* process/Reconciler: Reconcile records
* process/Merger: Merges two Linked Art records representing the same entity together.
* process/Reidentifier: Recursively external rewrite URIs in a record to internal identifiers, given an IdMap.
* process/ReferenceManager: Manages inter-record connections
* process/UpdateManager: Manages harvesting and updates to caches
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

Process:
1. In the config file, look up `dumpFilePath` and `remoteDumpFile`
2. Go to the directory where `dumpFilePath` exists and rename it with a date (e.g. latest-2022-07)
3. execute `wget <url>` where `<url>` is the URL from `remoteDumpFile` (and probably validate it by hand online)
4. For wikidata, as it's SO HUGE, instead do:  `nohup wget --quiet <url> &` to fetch it in the background so we can get on with our lives in the mean time.
5. Done :)
