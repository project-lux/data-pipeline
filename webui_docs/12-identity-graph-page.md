# W12. Identity graph page

**Size:** S · **Depends on:** W05, W06
**Read:** `lux_pipeline/cli/viz.py` (`graph_data` from W05, `replay`,
`enable_debug`, `node_labels`, `graph_edges`, `shortest_path`, and how
`process` builds the `Reconciler`), `refactor_docs/36-opt-viz-command.md`

## Intent

`lux viz` in the browser: two URIs in the same identity in, the graph of
how reconciliation connected them out, drawn as SVG with a key and the
shortest path highlighted.

| Method, path | What |
|---|---|
| `GET /graph?a=<uri>&b=<uri>` | the form (prefilled from the Records identity tab's *Graph with…* link) and, when both are given, the drawing |
| `GET /api/graph?a=&b=` | `records.identity_graph` plus layout coordinates |

## Design

`api/graph.py: graph_page_data(state, a, b)`:

1. `records.identity_graph(state.configs_instantiated_for(a, b), a, b)` —
   `viz.graph_data` needs the sources of both URIs instantiated
   (`state.source(...)` for each, resolved through `records.resolve`) and
   the idmap (`state.idmap()`); the replay reconciles in-process with
   `debug` on and `acquirer_should_store = False` (W05 kept that), so it
   writes nothing; it may fetch over the network when `allow_network` is
   on, as `lux viz` does — say so on the page.
2. Layout: `networkx.spring_layout(G, seed=1)` (deterministic), scaled to a
   viewBox of 1000×700 with a 40px margin; node radius 14; `ImportError`
   → 409 with the `pip install -e .[ui-extras]` hint.
3. Returns `{"nodes": [{"id", "uri", "source", "x", "y", "on_path": bool, "is_endpoint": bool}], "edges": [{"from", "to", "kind", "on_path"}], "path": [...], "yuid", "notes"}`.

Template: an inline `<svg>` — edges as lines (dashed for `nm`, solid for
`eq`, dotted for `uri`, thicker and coloured when on the path), nodes as
circles coloured by source (a stable palette by hash of the source name),
labels `source:n` beside each; hovering a node shows its URI (`<title>`);
clicking goes to `/records/r/<uri>`. Below: the key (label → URI) as a
table, the shortest path as a breadcrumb, and the edge kinds legend from
`lux viz`'s docstring ("eq" an equivalent in a record or found by the
crawl, "uri" an identifier index hit, "nm" a name index hit). Nothing
client-side beyond hover.

Runs in a thread (`to_thread`) with a 120 s timeout → 504 with "the
replay took too long; try `lux viz` at a shell".

## Known hazards

* `enable_debug` mutates the shared `Reconciler`'s state; build a fresh
  `Reconciler(cfgs, idmap, networkmap)` per request as `viz.process` does,
  never reuse one across requests.
* Two URIs not in the same identity: `lux viz` raises; show the message.
* The replay can be slow (it reconciles every member); the timeout above.

## Steps

1. `api/graph.py` with the layout; tests with `identity_graph` patched.
2. Template with SVG; link from the Records identity tab.
3. `docs/web.md` section.

Note (from W08): the record page's identity tab is where "Graph with…"
goes: a link beside each other member to `/graph?a=<this record>&b=<member>`
(`templates/records/journey.html`, the `identity` stage).

## Tests (`tests/test_web_graph.py`)

* With `records.identity_graph` patched to return a four-node graph with a
  path: `/api/graph` has coordinates within the viewBox, `on_path` marks
  the right nodes and edges; `/graph` contains four `<circle>` and the key
  rows; the same input gives the same coordinates twice (seeded layout).
* Missing `networkx` (block in `sys.modules`) → 409 with the hint.
* One URI only → the form without a drawing, 200.

## Acceptance criteria

* On the build machine (Rob): the AAT / Wikidata pair from `lux viz`'s
  docstring draws the same nodes and path as the terminal.
* Tests pass.

## As built

`web/api/graph.py`, `web/routers/graph.py`, `templates/graph/index.html`,
the graph styles in `lux.css`; the Graph area in the navigation after
Records, and *Graph with this* beside each other member on the record
page's identity tab (only when the record looked up is a member, not for
a YUID lookup). Differences from the design above:

* **No `configs_instantiated_for`.** The replay's `Reconciler` takes every
  external and results source's reconciler, and `typed_uri` / `replay`
  use any source's mapper and acquirer, so `replay` calls
  `AppState.build_all()` (each source built once) and `state.idmap()`
  (the same store `cfgs.get_idmap()` hands `graph_data`, its memory cache
  off). `records.identity_graph` and `viz.graph_data` are unchanged: a
  fresh `Reconciler` and `Collector` per call, as `lux viz` builds them.
* **The shared source reconcilers.** `viz.enable_debug` sets `debug` and
  `debug_graph` on the source reconcilers the server keeps. One replay
  runs at a time (`REPLAY_LOCK`), and each puts their `debug` and
  `debug_graph` back when it ends.
* **Layout and drawing**: `nx.spring_layout(seed=1)` scaled to 1000×700
  with a 40 margin (a single node, or a flat axis, is centred). Edges stop
  at the circles and have arrowheads (`<marker>`), since the debug graph
  is directed. All coordinates are worked out in Python. Colours are
  ten CSS classes (`.src-0` to `.src-9`) picked by `zlib.crc32` of the
  source name, not a per-process `hash`. The kinds are CSS classes (`eq`
  solid, `uri` dotted, `nm` dashed). The two URIs asked for are ringed.
  `notes` says when nothing was recorded, a URI is not in the graph, or
  the two are not connected.
* **Errors** on the page keep the form: 400 for URIs that are not one
  identity (or not in the idmap, or unknown), 409 without networkx (with
  the `pip install -e .[ui-extras]` hint), 504 after `TIMEOUT` (120 s).
  `/api/graph` gives the same codes as JSON. A replay that times out goes
  on in its thread, holding the lock, until it ends.
* Acceptance on the laptop (2026-11-02): the AAT / Wikidata pair from `lux
  viz`'s docstring gives 17 URIs and 21 connections. The URIs, the
  connections and the shortest path (aat → wikidata) are the same as
  `viz.graph_data` in a shell, compared by URI. The label numbers differ
  between runs, in the shell too: they follow the order the replay meets
  URIs, which is not fixed.

