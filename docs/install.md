# Installing the LUX pipeline

From a fresh machine to `lux counts`. The pipeline needs Python 3.10 or
later and PostgreSQL. Redis is only needed if one of your map records
still names it (the build machine's `networkmap`, `record_refs` and
`redirects` may); `lux testinstall` says so if it is.


## 1. System packages

### Ubuntu

From a brand new server (e.g. an instance in Spinup if you're at Yale), as
root:

```
apt install postgresql          # or the latest from https://www.postgresql.org/download/linux/ubuntu/
apt install redis               # only if a map record uses it
apt install python3-pip python3-venv git
su - postgres
createuser <user>
createdb <user>
```

### Mac

* Install Postgres: https://postgresapp.com/ works well
* Optionally pgAdmin: https://www.pgadmin.org/download/pgadmin-4-macos/
* Install Homebrew (https://brew.sh/), then `brew update`
* If needed, Redis: `brew install redis`
* Set Postgres (and Redis) to start at login


## 2. The code

As your own user:

```
mkdir lux
cd lux
python3 -m venv ENV
source ENV/bin/activate
pip install --upgrade pip
git clone https://github.com/project-lux/data-pipeline.git
cd data-pipeline
pip install -e .[dev]           # the lux_pipeline package, the `lux` command, and the test tools
cd ..
```

`pip install -e .[ray]` adds the Ray engine (`--engine ray`); it is not
needed otherwise. `lux help` lists the commands, and `lux <command>
--help` the options of one.


## 3. An installation

`lux initialize` creates the data directories, installs a configuration,
writes the `.env` that points `lux` at it, fetches the Linked Art schemas
the validator reads and creates the database tables. Pick one source of
configuration:

```
lux initialize ~/lux/data                                  # the samples in docs/sample_config
lux initialize ~/lux/data --configs ~/lux/pipeline-configs # a pipeline-configs checkout you already have
lux initialize ~/lux/data --clone                          # clone project-lux/pipeline-configs into it
```

Every step checks its own state, so if one fails (Postgres not running,
no network) fix the cause and run the same command again; nothing already
in place is overwritten. Then edit the installed `base.json` / `system.json`
for this machine (paths, `max_workers`) if the defaults are not right, and
check everything:

```
cd ~/lux/data
lux testinstall
```

It prints one line per check and stops at the first failure: the `.env`,
the configuration, Postgres (and Redis where configured), the idmap, the
sources' components, the data directories, the schemas, and that worker
processes can be started.

`lux` finds its configuration through the nearest `.env`, searched from
the current directory upwards (or `LUX_BASEPATH` in the environment).
`lux initialize` writes it into the installation directory; the build
scripts run from the repository, so copy it there too:

```
cp ~/lux/data/.env ~/lux/data-pipeline/
```


## 4. Data

Download the published LUX export (the records the last build used) and
the sources' own dump files, then load the export into the datacaches:

```
lux download --source all --type export --max-workers 10
lux download --source all --type records --max-workers 10
lux load --source all --type export --max-workers 10
```

Use at least 10 workers for the downloads, more if the CPU and network
can take it. A file that fails is logged and the rest carry on; the
command exits non-zero at the end, and running it again with
`--no-overwrite` fetches only what is missing.

To load a source's own dump instead (`--type records`, the default), and
to build the reconciliation indexes from the datacaches:

```
lux load --source wikidata --max-workers 24
lux load-index --source all
```

Then see what is there:

```
lux counts
lux list                    # the configured sources; --maps, --indexes
```


## 5. A first build

A new identity map needs an update token and the global terms before the
first reconcile:

```
lux new-token
lux reconcile --baseline
```

and then a build, as described in [rebuild.md](rebuild.md), from the
repository directory:

```
cd ~/lux/data-pipeline
./run-all2.sh --all
```

The script writes its per-slice output to `../data/logs`, which is the
configured `log_dir` with the layout above (`~/lux/data` next to
`~/lux/data-pipeline`, and a pipeline-configs installation, whose
`log_dir` is `logs`); set `LOGS=<log_dir>` when yours is elsewhere.

`lux watch --latest` shows a running build from any terminal.
