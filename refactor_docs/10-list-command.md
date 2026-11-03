# 10. `lux list`

**Priority:** P1 · **Size:** S · **Depends on:** 05
**Read from:** `rob_claude_refactor:lux_pipeline/cli/list.py` (162 lines; the file exists only on that branch)

## Intent

Add `lux_pipeline/cli/list.py`: a Rich table of the configured sources
(internal / external / results) with namespace, merge order and the
configured classes. It is the quickest way to see what a configuration
directory actually contains.

```
lux list                 # all three groups
lux list --source external
lux list --source aat,lcnaf
```

## Files

| Action | Path on `main` | Source |
|---|---|---|
| Create | `lux_pipeline/cli/list.py` | `rob_claude_refactor:lux_pipeline/cli/list.py` |
| Create | `tests/test_cli_list.py` | new |

## What to port

The whole file, verbatim. Add `"downloaderClass"` and `"indexLoaderClass"`
to `_CLASS_KEYS`. Python 3.10+ typing (`dict | None`) is fine with the
`requires-python` set in item 05.

## Steps

1. Copy, fix imports (`from ._handler import BaseHandler as BH` is unchanged).
2. Test with a stub config and `Console(file=io.StringIO())` — assert the
   source names and class names appear.

## Acceptance criteria

* `lux list` renders the live instance's sources.
* Tests pass.
