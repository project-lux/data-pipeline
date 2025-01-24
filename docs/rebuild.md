
# How to Rebuild the Dataset with the Pipeline


## 1. Ensure data caches are up to date

### 1.1. Fetch the latest dump files for sources that have them

* Sources with remote dump files are given in the configuration for the source in the `remoteDumpFile` config field. 
* Run ./scripts/checkDataUpdates.py to automatically compare the timestamp of the local copy with the timestamp of the remote file, or individual records with the timestamps in the activitystream or in the remote repository, if possible.
* Download them into the directory, per the output of the checking script, using curl or wget (or other tool as appropriate)
  * Example: `nohup wget https://dumps.wikimedia.org/wikidatawiki/entities/latest-all.json.gz &`
* Sources that have datasets in S3 may need special handling
  * Example: `nohup aws s3 --profile <profile> sync <bucket> <directory> &`

### 1.2. Load them with loader for that source

* Use: `./scripts/manage.py --load [name-of-source]`  This will first clear the source's data cache, and then call `load()` on the loader function to re-ingest the downloaded dump file.
* When it finishes, you can check the number of records with `./scripts/manage.py --counts`
* If it hasn't loaded, check for any errors, and whether the file is in the right place with the right name

### 1.3. Consider caches without dump files

* If it has been a long time and there are only relatively few records, it might be worth running a refresh on a dataset to reload the records that have been used.
* Use `./scripts/refresh.py --load [name-of-source]` to reload the records in datacache that are currently in the recordcache (e.g. those ones that have been transformed into linked art for use)
* The time needed will vary dramatically both per source, and for the number of records.

### 1.4. Rebuild inverted indexes

* If a cache has changed, then you'll want to rebuild the indexes used for reconciliation.
* Use `./scripts/manage.py --index [name-of-source]`
* To rebuild all indexes, use `all` as the name of the source

### 1.5. Fetch the latest internal records

* Run the harvesters for the internal sources
* Use `./scripts/manage.py --harvest [name-of-source]` to run over the activity streams to harvest the latest records
* These can take a long time too. For big change sets, loading a dumpfile is faster.
* As above, use `./scripts/manage.py --counts` to verify

### 1.6. Consider backing up the identifier map database

* If we lose the idmap database in redis, then we have a big problem!
* If there's sufficient disk space, copy the redis db which is very likely to be up to date on disk.
* YALE: In the pipeline machine, the db is in /data-io2/redisdata/dump/ and copy to /data/previous/dump-DATE.rdb

### 1.7. Regenerate sameAs / differentFrom tables as needed

 * Wikidata will have new differentFroms frequently, whereas AAT will not
 * YALE: Fetch the google spreadsheet values as CSV to load for internal sames/differents


### 1.8. Reload the sameAs / differentFrom tables that changed

* These live along side the identifier map, hence the backup in the previous step
* Use `./scripts/load-csv-map.py --same|--different [--pipe] <path/to/file.csv>` where:
	* --same will load to the sameAs table
	* --different will load to the differentFrom table
	* --pipe will use | as the separator between columns
	* and then the filename for the CSV to load
* These should all live in the data directory: `data/files/filename.csv`

### 1.9. Ready to build!

 * You're now up to date with all the data changes in the environment and ready to build


## 2. Build the Dataset

### 2.1. Re-consider making backups

 * If something goes horribly wrong, how hard will it be to recover? If hard ... make a backup.
   * Especially the identifier map database, as everything else isn't a System of Record

### 2.2. Transform and Reconcile

### 2.3. Reidentify and Merge

### 2.4. Move the previous dataset 

 * Copy the current latest dataset (likely in production) to a dated directory
    * YALE: This is /data-export/output/lux/latest
 * Compress the previous dated dataset
    * e.g. `nohup gzip -1 latest-2023-05-02/* &`

### 2.5. Export the Data

### 2.6. Pre-load Marklogic Steps

### 2.7. Load to Marklogic





