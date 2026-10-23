# 28. `lux initialize` and `lux testinstall`

**Priority:** P2 · **Size:** S · **Depends on:** 05
**Read from:** `rob_refactor:lux_pipeline/cli/initialize.py` (78 lines), `rob_refactor:lux_pipeline/cli/testinstall.py` (62 lines), `rob_refactor:install.md`

## Intent

```
lux initialize [dir]     # create the directory layout + a config_cache from docs/sample_config + a .env
lux testinstall          # diagnose: .env? LUX_BASEPATH? Config builds? idmap reachable? caches reachable?
```

Both run with `cfgs=None` (item 05 allows exactly these two plus `help`).

## Files

| Action | Path on `main` | Source |
|---|---|---|
| Create | `lux_pipeline/cli/initialize.py` | `rob_refactor:lux_pipeline/cli/initialize.py` |
| Create | `lux_pipeline/cli/testinstall.py` | `rob_refactor:lux_pipeline/cli/testinstall.py` |
| Create | `tests/test_cli_initialize.py` | new |

## What to port

`initialize`: the directory creation (`files`, `logs/flags`, `indexes`,
`input`, `output`, `tmp`, `config_cache`) and the `.env` write. **Replace**
the `git clone` / PyGithub tarball download of `project-lux/pipeline-configs`
with copying `docs/sample_config/*.json` (shipped in this repository) into
`<dir>/config_cache/`, then rewriting `base_dir` in `base.json` to the
absolute target. Print the next steps (edit `pg_*`, run `lux testinstall`).
Refuse to run if the directory already has a `config_cache`.

`testinstall`: port as is, but make each check print `OK`/`FAIL` on one
line and exit non‑zero on the first failure; use `lux_pipeline.runtime`
functions instead of re‑implementing dotenv handling. Add a check that
`cfgs.max_workers` processes can be spawned (an `MpEngine` run of a no‑op
manager with `prefix` under `tmp`, asserting the `.status` files appear)
once item 07 exists.

## Known bugs in the source — do not copy

* `initialize` does `os.chdir(where)` before `p.mkdir()`; order matters when the directory does not exist yet.
* Bare `except:` around `subprocess.run(["git", "clone", …])`.
* `testinstall` has `raise` followed by unreachable `return`.

## Acceptance criteria

* `lux initialize /tmp/luxtest && cd /tmp/luxtest && lux testinstall` reports the Postgres check as the first failure on a machine without Postgres, and `OK` throughout on the live instance.
* Test covers the directory layout and the `.env` content with `tmp_path`.
