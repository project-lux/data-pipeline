
## Codebase Notes

Ordered from Easiest to Most Diabolical to understand

### Code Layout

Data source specific code is in sources/, divided by organization that publishes the data.

The rest of the code is divided among directories according to function: 
* cache: store records
* collector: follow links in records to collect more records
* config: handle configuration files
* harvester: harvest records from activitystreams
* idmap: manage and store identifier alignment as a giant hash (internal id to external ids, external id to internal id)
* marklogic: (should maybe be refactored) manage interactions with marklogic
* merger: merge records that describe the same entity down to one record
* reconciler: given a record, find other records that describe the same entity
* validator: validate the JSON structure of a record

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



