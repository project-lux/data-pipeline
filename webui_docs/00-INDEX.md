# Index — work top to bottom

Read `DESIGN.md` first. W00 is a test fixture (a database-free
installation) that W04 onwards build their tests on. Items W01–W05 change
existing modules so that the console and the web read the same data; they
have no web code and each must leave `lux` behaving exactly as before. W06
is the server skeleton. W07–W12 are the pages and are independent of each
other. W13 is last.

Size is the rough amount of new/changed code: S < 150 lines, M < 500, L
more. Tick the box in the PR that lands the item.

| # | Document | Size | Depends on | Done |
|---|---|---|---|---|
| W00 | [A database-free test installation](00-test-installation-fixture.md) | S | — | ☑ |
| W01 | [Run files: shared reader, `.run` records, `list_runs`](01-run-files-and-shared-state.md) | M | — | ☑ |
| W02 | [Command introspection (`cli/introspect.py`)](02-command-introspection.md) | S | — | ☑ |
| W03 | [Data functions for counts, testinstall and list](03-data-functions-counts-checks.md) | S | — | ☑ |
| W04 | [Config store, key catalogue and `lux config`](04-config-store-and-schema.md) | M | W00 | ☑ |
| W05 | [Record inspection service and `lux show`](05-record-inspection-service.md) | M | W00 | ☑ |
| W06 | [Server skeleton, `lux serve`, dashboard, health](06-server-skeleton-and-dashboard.md) | L | W00, W01, W03 | ☐ |
| W07 | [Configuration pages](07-config-pages.md) | L | W04, W06 | ☐ |
| W08 | [Record pages](08-record-pages.md) | L | W05, W06 | ☐ |
| W09 | [Run pages and live progress](09-run-pages-live-progress.md) | L | W01, W06 | ☐ |
| W10 | [Command launcher and the build page](10-command-launcher-and-build.md) | L | W02, W09 | ☐ |
| W11 | [Timing, counts and maintenance pages](11-timing-and-maintenance-pages.md) | M | W03, W06 | ☐ |
| W12 | [Identity graph page](12-identity-graph-page.md) | S | W05, W06 | ☐ |
| W13 | [Security, action log, docs](13-docs-security-hardening.md) | M | W07–W12 | ☐ |

## Dependency picture

```
W00 test installation ──► W04, W05, W06 (their tests build a real Config on it)

W01 run files ──┬──────────────► W06 skeleton ──┬─► W07 config pages  (needs W04)
W03 data fns  ──┘                               ├─► W08 record pages  (needs W05)
                                                ├─► W09 run pages ──► W10 launcher + build (needs W02)
W02 introspect ─────────────────────────────────┤
W04 config store ───────────────────────────────┼─► W11 timing / counts / maintenance (needs W03)
W05 records ────────────────────────────────────┴─► W12 identity graph (needs W05)
                                                                 │
                                       W07 … W12 ──────────────► W13 hardening + docs
```

W00–W03 can be done in parallel by different people, and W04 and W05 once
W00 is in. W07–W12 can be done in parallel once W06 is merged.
