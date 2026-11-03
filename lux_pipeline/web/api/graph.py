"""The identity graph: `lux viz` in the browser. Two URIs of one identity
in; how reconciliation connected them out (`records.identity_graph`, the
same `cli.viz.graph_data` the command draws), laid out for an SVG.

The replay reconciles every record of the identity again in this
process, with debug recording on and nothing stored; with
``allow_network`` on it may fetch records it does not have. It needs every
source's reconciler, so the first graph builds them all
(`AppState.build_all`), and `viz.enable_debug` switches debug on in those
shared source reconcilers: one replay runs at a time, and each puts their
debug settings back when it ends.
"""

import asyncio
import threading
import zlib

from fastapi import APIRouter, HTTPException, Request

from lux_pipeline import records

router = APIRouter(prefix="/api/graph", tags=["graph"])

WIDTH, HEIGHT, MARGIN, RADIUS = 1000, 700, 40, 14
PALETTE = 10                     # node colours: lux.css .src-0 ... .src-9
TIMEOUT = 120.0
INSTALL_HINT = "pip install -e .[ui-extras]"
TIMEOUT_MESSAGE = "the replay took too long; try lux viz at a shell"
KINDS = {"eq": "an equivalent in a record, or found by the collector's crawl",
         "uri": "an identifier index hit", "nm": "a name index hit"}

REPLAY_LOCK = threading.Lock()


class GraphTimeout(Exception):
    pass


def need_networkx():
    """networkx, or RuntimeError (409) saying how to install it."""
    try:
        import networkx
    except ImportError:
        raise RuntimeError(f"the identity graph needs networkx: {INSTALL_HINT}") from None
    return networkx


def colour(source):
    """A palette class for `source`, the same on every page and run."""
    return zlib.crc32(source.encode("utf-8")) % PALETTE


def layout(nodes, edges):
    """``{label: (x, y)}`` inside the viewBox, margins kept: networkx's
    spring layout, seeded so a graph is drawn the same way every time."""
    nx = need_networkx()
    g = nx.Graph()
    g.add_nodes_from(n["id"] for n in nodes)
    g.add_edges_from((e["from"], e["to"]) for e in edges)
    if not len(g):
        return {}
    pos = nx.spring_layout(g, seed=1)
    xs = [p[0] for p in pos.values()]
    ys = [p[1] for p in pos.values()]

    def scale(v, lo, hi, size):
        if hi - lo < 1e-9:
            return size / 2
        return MARGIN + (v - lo) / (hi - lo) * (size - 2 * MARGIN)

    return {k: (round(scale(p[0], min(xs), max(xs), WIDTH), 1),
                round(scale(p[1], min(ys), max(ys), HEIGHT), 1)) for k, p in pos.items()}


def _shorten(x1, y1, x2, y2, by):
    """The line between two node centres, ending at their circles."""
    dx, dy = x2 - x1, y2 - y1
    d = (dx * dx + dy * dy) ** 0.5
    if d <= 2 * by:
        return x1, y1, x2, y2
    fx, fy = dx / d * by, dy / d * by
    return round(x1 + fx, 1), round(y1 + fy, 1), round(x2 - fx, 1), round(y2 - fy, 1)


def shape(data):
    """`viz.graph_data`'s answer with what the drawing needs: coordinates,
    whether each node and edge is on the shortest path, the ends, the
    colour classes; the arithmetic is done here, not in the template."""
    pos = layout(data["nodes"], data["edges"])
    path = data.get("path") or []
    on_path_edges = {frozenset(p) for p in zip(path, path[1:])}
    ends = [e for e in data.get("ends") or [] if e]
    nodes = []
    for n in data["nodes"]:
        x, y = pos.get(n["id"], (WIDTH / 2, HEIGHT / 2))
        nodes.append({**n, "x": x, "y": y, "on_path": n["id"] in path,
                      "is_endpoint": n["id"] in ends, "colour": colour(n["source"]),
                      "label_x": round(x + RADIUS + 4, 1), "label_y": round(y + 4, 1)})
    by_id = {n["id"]: n for n in nodes}
    edges = []
    for e in data["edges"]:
        a, b = by_id.get(e["from"]), by_id.get(e["to"])
        if a is None or b is None:
            continue
        x1, y1, x2, y2 = _shorten(a["x"], a["y"], b["x"], b["y"], RADIUS + 2)
        edges.append({**e, "on_path": frozenset((e["from"], e["to"])) in on_path_edges,
                      "x1": x1, "y1": y1, "x2": x2, "y2": y2})
    notes = []
    if not data["nodes"]:
        notes.append("Reconciling the identity's records recorded no connections.")
    for uri, end in zip((data.get("from"), data.get("to")), data.get("ends") or []):
        if end is None and data["nodes"]:
            notes.append(f"{uri} is not in the graph.")
    if ends and len(ends) == 2 and not path:
        notes.append("The two are not connected in the graph.")
    return {"yuid": data.get("yuid"), "from": data.get("from"), "to": data.get("to"),
            "nodes": nodes, "edges": edges, "path": path, "ends": data.get("ends"),
            "notes": notes, "width": WIDTH, "height": HEIGHT, "radius": RADIUS}


def replay(state, a, b):
    """`records.identity_graph` on the server's configuration with every
    source built; one at a time, the source reconcilers' debug settings
    put back afterwards."""
    with REPLAY_LOCK:
        cfgs = state.build_all()
        state.idmap()                 # the map the replay reads, its memory cache off
        saved = []
        for group in ("external", "results"):
            for cfg in getattr(cfgs, group).values():
                r = cfg.get("reconciler")
                if r is not None:
                    saved.append((r, getattr(r, "debug", False), getattr(r, "debug_graph", None)))
        try:
            return records.identity_graph(cfgs, a, b)
        finally:
            for r, debug, graph in saved:
                r.debug = debug
                r.debug_graph = graph if graph is not None else {}


async def graph_data(state, a, b, timeout=None):
    """The drawing of `a` and `b`'s identity. RuntimeError without
    networkx, ValueError for URIs that are not one identity, GraphTimeout
    after `timeout` seconds (the replay goes on in its thread, holding the
    lock, until it is done)."""
    need_networkx()
    try:
        data = await asyncio.wait_for(asyncio.to_thread(replay, state, a, b),
                                      TIMEOUT if timeout is None else timeout)
    except (asyncio.TimeoutError, TimeoutError):
        raise GraphTimeout(TIMEOUT_MESSAGE) from None
    return shape(data)


@router.get("")
async def get_graph(request: Request, a: str, b: str):
    try:
        return await graph_data(request.app.state.lux, a.strip(), b.strip())
    except GraphTimeout as e:
        raise HTTPException(504, str(e)) from None
