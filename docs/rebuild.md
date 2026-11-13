# How to Rebuild the Dataset with the Pipeline

Every step is a `lux` command (`lux help` lists them, `lux <command>
--help` gives the options). They need the installation's `.env`, in
`~/.lux/` or the directory they run from (see [install.md](install.md)). Parallel
commands take `--max-workers N` (default: `max_workers` in the base
configuration) and show a console view; `lux watch --latest` attaches to
a running build from another terminal. Any of them takes `--background`
(`-b`): it starts as a process of its own, survives the terminal closing,
and `lux` returns at once with the `lux watch` command that follows it and
the `kill` that stops it; its output goes to `<prefix>/stdout.log`.


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
 * WIKIDATA: `lux build-differents --source wikidata --max-workers 24` writes
   `data/differentFrom/wikidata.csv` from the datacache (`P1889`, and every
   pair of items on a disambiguation page); `--load` also adds it to the
   differentFrom table, as step 1.7 does
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

Or `lux build` (the same phases, one `lux` process each), from a shell
(`lux build --background` to leave it running, rather than `nohup`) or
from the web UI: `lux serve`, then *Commands* → *the build page*, which
shows the steps (`lux build --dry-run`) before starting, and the build's
run page with its phases as they go ([web.md](web.md)). Every run, from
a shell or the web, is on the *Runs* page as `lux watch <prefix>` shows
it in a terminal: the two read the same files under `log_dir`.

### 2.4. Or step by step: transform and reconcile

```
lux reconcile --source all --max-workers 24
```

reconciles every internal source and the references they lead to, then,
once every worker has finished, resolves the identity map (`lux
identify`), merges the metatypes (`lux metatypes`) and writes the
referenced URIs (`lux write-refs`). With `--no-finish` those three are
left to run by hand, in that order.

Identify's last line reports the identity map's presence changes: YUIDs
no cluster took this build (their records are gone from the sources),
YUIDs that came back, YUIDs registered. A missing YUID keeps its member
rows so a record that returns gets its old identity; drop the ones that
have been missing long enough with `lux pg-maintain --expire-yuids 180`
(`--dry-run` counts first). Identify's sorts take `25%` of memory by
default (`LUX_SORT_BUFFER=40G` to say otherwise) and merge the slices'
files, which each slice sorts as it finishes.

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
`lux export --source ycba --cache datacache` dumps a whole cache instead
(`--max-workers N` splits it), for `lux load --file <dir> --file-type
dir/lines.gz/tabjson` on another instance.

`lux export --type merged --format lmdb --max-workers 24` builds the LMDB
distribution store (`merged.lmdb` in `distribution_dir`: every record
under its UUID, and an index by class); `--type external` or `--cache`
with `--format lmdb` build `<source>:<id>`-keyed stores. Run as
`--my-worker` slices, finish with the same command and `--merge-only`.
The workers write sorted runs (`LUX_LMDB_RUN_MB`, default 256, of records
each); the command then merges them into the store, reading every run
through its share of a `LUX_LMDB_READ_MB` budget (default 1024, between 1
and 16 MB a run), so the merge reads in large pieces rather than many
small ones. Its log gives the rate over the last million records as well
as the overall one.


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

Whatever the engine, a run is the directory
`<log_dir>/<command>-<timestamp>/`: every worker writes `w<n>.status` and
`w<n>.log` in it, and the command itself logs to its `driver.log`; that is what
`lux watch` reads, and the logs are where a worker's errors are. The
command's own log is where its tail is: run alone, `lux reconcile`'s
identify, metatypes and write-refs run after the last worker, and `lux
watch` stays on them until the run's record says it is over. In a `lux
build` they are a phase of their own, `identify` (reconcile runs with
`--no-finish`), so watch and the run page say identify while it runs, and
`lux build --from identify` carries a build on after an identify that
failed. `lux watch <log_dir>/build-<timestamp>`
follows a `lux build` from step to step, the one-shot steps included
(each is a run in the build's directory, `<build prefix>/<phase>/`, with
its own `driver.log`); `lux watch --latest`
picks the build when the newest run is one of its phases.


## Where the time went

Every phase writes `timing-<phase>-<slice>.json` into its run's
directory, beside `w<n>.timing`; `lux timing-report` finds each phase's
latest run under `log_dir` (`--dir` a run's or a build's directory for
that one) and adds the slices up, stage by stage, phase by phase in
build order, and ends with all the phases together: start to end, the time
between phases, and the stages that cost the most elapsed time. Beside the
workers' own cpu it shows what postgres used and how busy the machine was
(iowait on Linux): a worker waiting on postgres looks idle from inside (`--compare
old/ new/` sets two runs side by side; `lux --no-ui timing-report` prints
plain text for a log or a diff). To see which functions inside a
stage are slow, run the phase with `--profile` (or `LUX_PROFILE=1` in the
environment, which the legacy scripts honour too), then
`lux timing-report --profile`. Profiling slows the phase down, so
don't compare a profiled run's timings with an unprofiled one.
