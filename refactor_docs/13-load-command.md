# 13. `LoadManager` and `lux load`

**Priority:** P1 · **Size:** S · **Depends on:** 07, 12 · **Unblocks:** 29, 31
**Read from:** `rob_claude_refactor:lux_pipeline/process/tasks/load_task.py`, `rob_claude_refactor:lux_pipeline/cli/load.py`; `main:manage-data.py` lines 49–94 (`--load`) and `import_parallel.sh`

## Intent

```
lux load --source ror                         # DSL or legacy load(), one process
lux load --source wikidata --max-workers 24   # 24 workers, each a slice
lux load --source all --type export           # load a LUX export (load_export2) into datacaches
lux load --source ycba --no-overwrite
```

`LoadManager._distributed(n)` prepares and runs each selected source's loader
for slice `n`. The command replaces `manage-data.py --load` and
`import_parallel.sh`.

## Files

| Action | Path on `main` | Source |
|---|---|---|
| Create | `lux_pipeline/process/tasks/load_task.py` | `rob_claude_refactor:lux_pipeline/process/tasks/load_task.py` |
| Create | `lux_pipeline/cli/load.py` | `rob_claude_refactor:lux_pipeline/cli/load.py` |
| Create | `tests/test_load_task.py` | new |

## What to port

Both files, with the compatibility dispatch in `_distributed`:

```python
def _distributed(self, n, cfgs, progress):
    for which, src in self.sources:
        ldr = getattr(cfgs, which)[src]["loader"]
        ldr.prepare(n, self.max_workers, self.load_type, self.max_records, progress=progress)
        if self.load_type == "export":
            ldr.load_export2()                      # main's export loader; single process is fine
        elif ldr.uses_dsl:
            ldr.process(overwrite=self.overwrite)
        elif self.max_workers > 1 and _accepts_slices(ldr.load):   # inspect.signature has 2 params
            ldr.load(n, self.max_workers)
        elif n == 0:
            ldr.load()                              # legacy single-process loader; other workers idle
```

Options (`--type records|export|lmdb`, `--no-overwrite`, `--max-records`)
are read from `args` in `LoadManager.__init__`, as `rob_claude_refactor`
does; `make_manager` is one line.

`maybe_add` keeps the refactor's `isinstance(cfg["loader"], Loader)` check.
`--type` accepts `records|export|lmdb`.

Clearing: `manage-data.py --load` clears the datacache first for the simple
sources. Add `--clear` (default off) that calls `cfg["datacache"].clear()`
in the **driver** before dispatching (never inside a worker).

## Known bugs in the source — do not copy

* `load_task._distributed` sleeps 1 s on every exit — remove.
* `cli/load.py` `--no-overwrite` uses `action="store_false"` into
  `no_overwrite`, then maps `overwrite: "no_overwrite"` — works but
  confusing; use `dest="overwrite", action="store_false"`.

## Steps

1. Create both files (docstring on the command module).
2. `tests/test_load_task.py`: stub config with a `FakeLoader` recording
   `prepare`/`process`/`load` calls; use `manager.run_single()` (null
   engine, item 07) to assert the dispatch rules above.
3. Live: `lux load --source ror --no-ui` vs `manage-data.py --load --ror`.

## Acceptance criteria

* The three dispatch paths are tested.
* `lux load --source wikidata --max-workers 4 --no-ui` runs four slices of the existing `WdLoader.load(slice, max)`.
