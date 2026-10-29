# How to Rebuild the Dataset with the Pipeline

Every step is a `lux` command (`lux help` lists them, `lux <command>
--help` gives the options). Run them from the repository directory, which
needs the installation's `.env` (see [install.md](install.md)). Parallel
commands take `--max-workers N` (default: `max_workers` in the base
configuration) and show a console view; `lux watch --latest` attaches to
a running build from another terminal.


## 1. Ensure data caches are up to date

### 1.1. Fetch the latest dump files for sources that have them

* Which files a source needs comes from its configuration
  (`remoteDumpFile` + `dumpFilePath`, or the `input_files` block).
* `lux download --source wikidata` fetches one source's files;
  `lux download --source all --type records --max-workers 10` fetches every
  source's. `--no-overwrite` keeps files already there.
* A file that fails is logged and the rest carry on; the command exits
  non-zero at the end.
* Sources that have datasets in S3 may need special handling
  * Example: `nohup aws s3 --profile <profile> sync <bucket> <directory> &`

### 1.2. Load them with the loader for that source

* `lux load --source <name> --clear` empties the source's datacache and
  loads the downloaded dump file into it; big dumps take `--max-workers 24`
  (each worker loads one slice).
* When it finishes, check the number of records with `lux counts --source <name>`.
* If it hasn't loaded, check for any errors, and whether the file is in
  the right place with the right name (`lux guess-dump-type --source
  <name>` shows how the files on disk would be described).

### 1.3. Rebuild the reconciliation indexes

* If a cache has changed, rebuild the indexes used for reconciliation:
  `lux load-index --source <name>`.
* To rebuild every index, use `--source all`.
* `lux counts --index <name>` shows their sizes.

### 1.4. Fetch the latest internal records

* Run the harvesters for the internal sources: `lux harvest --source <name>`
  walks the activity stream back to the last harvest.
* `lux harvest --source all --two-phase --max-workers 12` lists the changed
  records first and then fetches them in parallel.
* These can take a long time too. For big change sets, loading a dump file
  is faster.
* As above, use `lux counts` to verify.

### 1.5. Back up the identifier map

* If we lose the identifier map, we have a big problem!
* `lux backup --map idmap` copies it (and `lux backup --all` every cache,
  map and index) to `backup_dir`; `lux restore <dir> --map idmap --force`
  puts it back. Check the free space first.

### 1.6. Regenerate sameAs / differentFrom tables as needed

 * Wikidata will have new differentFroms frequently, whereas AAT will not
 * YALE: `python ./google-sames-diffs.py` fetches the Google spreadsheet of
   internal sames/differents as CSV (`run-all2.sh` does this at its start)

### 1.7. Reload the sameAs / differentFrom tables that changed

* These live alongside the identifier map, hence the backup in step 1.5
* Use `python ./load-csv-map2.py --same|--different <path/to/file.csv>`, where:
	* --same loads to the sameAs table
	* --different loads to the differentFrom table
	* and then the filename of the CSV to load
* These should all live in the data directory: `data/files/filename.csv`

### 1.8. Ready to build!

 * You're now up to date with all the data changes in the environment and ready to build


## 2. Build the Dataset

### 2.1. Re-consider making backups

 * If something goes horribly wrong, how hard will it be to recover? If
   hard ... make a backup (`lux backup`, step 1.5).
   * Especially the identifier map, as everything else isn't a System of Record

### 2.2. Start a new build

 * Clear the computed caches and the reference queues, and mint a new
   update token:

   ```
   lux clear --source all --force
   lux clear --refs --force
   lux new-token
   ```

 * A new identity map also needs the global terms: `lux reconcile --baseline`.

### 2.3. The whole build: `run-all2.sh`

`./run-all2.sh --all` (or `--<source>`) runs everything below: it reloads
the Google sames/diffs, then reconcile, identify, metatypes, write-refs,
merge, `post-build-portal.py` and export. Start it with `nohup`. On an
error it stops with exit status 1 and appends a line to
`/data/logs/pipeline_process_status.txt` (`STATUS_FILE`); each phase
prints the `lux watch <prefix>` command that shows it.

(`run-all2.sh` is the `lux` version of `run-all.sh`; the two sit side by
side until builds with both have been compared, after which the old one
goes and the new one takes its name.)

### 2.4. Or step by step: transform and reconcile

```
lux reconcile --source all --max-workers 24
```

reconciles every internal source and the references they lead to, then,
once every worker has finished, resolves the identity map (`lux
identify`), merges the metatypes (`lux metatypes`) and writes the
referenced URIs (`lux write-refs`). With `--no-finish` those three are
left to run by hand, in that order.

### 2.5. Reidentify and merge

```
lux merge --source all --max-workers 24
python ./post-build-portal.py --no-tqdm     # tags the YPM records
```

### 2.6. Move the previous dataset

 * Copy the current latest dataset (likely in production) to a dated directory
    * YALE: This is /data-export/output/lux/latest (`exports_dir`)
 * Compress the previous dated dataset
    * e.g. `nohup gzip -1 latest-2023-05-02/* &`

### 2.7. Export the data

```
lux export --max-workers 24                 # MarkLogic JSONL in exports_dir
lux export --type nt --max-workers 24       # N-Triples in nt_dir, for QLever
```

`lux export --help` lists the other types (`merged`, `sources`, `external`).


## Choosing an engine

Every parallel command runs its workers on an engine, chosen with
`--engine`:

* `mp` (the default): one process per worker, started and watched by the
  command itself. One command per phase.
* `null`: the worker runs in the command's own process. With
  `--my-worker i --max-workers N` that is one slice of N, so a shell loop
  can start the slices separately under `nohup`, as the `*_parallel2.sh`
  scripts and `run-all2.sh` do (their default, `ENGINE=null`).
  `ENGINE=mp ./run-all2.sh --all` runs each phase as one `mp` command instead.
* `ray`: workers as Ray tasks, on a local Ray or the cluster
  `RAY_ADDRESS` names. Needs `pip install -e .[ray]`.

Whatever the engine, every worker writes
`<log_dir>/<command>-<timestamp>-w<n>.status` and `.log`; that is what
`lux watch` reads, and the logs are where a worker's errors are.


## Where the time went

Every phase writes `timing-<phase>-<slice>.json` to `log_dir`;
`python timing-report.py` adds the slices up, stage by stage (`--compare
old/ new/` sets two runs side by side). To see which functions inside a
stage are slow, run the phase with `--profile` (or `LUX_PROFILE=1` in the
environment, which the legacy scripts honour too), then
`python timing-report.py --profile`. Profiling slows the phase down, so
don't compare a profiled run's timings with an unprofiled one.
