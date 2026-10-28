"""Draw how reconciliation connected two records of one identity.

    lux viz --recid http://vocab.getty.edu/aat/300033618,http://www.wikidata.org/entity/Q3305213

Both URIs must already be in the same identity (the idmap gives their
YUID). The identity's records are reconciled again in this process, the
internal ones first and then the smallest sources, with
`debug_reconciliation` switched on for this run only: the reconcilers and
the collector record which URI led to which ("eq" an equivalent in a
record or found by the collector's crawl, "uri" an identifier index hit,
"nm" a name index hit), and that graph is drawn in the terminal, followed
by a key from node label to URI and the shortest path between the two.
Records fetched over the network for this are not stored in the caches
(a failed fetch is still noted in the networkmap, as in a build).

Needs `netext` and `networkx` (``pip install -e .[ui-extras]``).
`debug-reconcile.py` draws the same graph with matplotlib.
"""

import logging
from collections import deque

from ._handler import BaseHandler

logger = logging.getLogger("lux_pipeline")

# namespaces that are not sources but turn up as equivalents
EXTRA_NAMESPACES = {"http://id.worldcat.org/fast": "fast"}


def typed_uri(cfgs, uri):
    """The ``<uri>##qua<Type>`` key the idmap holds for `uri`."""
    if cfgs.is_qua(uri):
        return uri
    split = cfgs.split_uri(uri)
    if not split:
        raise ValueError(f"Unknown URI (no configured source matches it): {uri}")
    src, ident = split
    ref = src["mapper"].get_reference(ident)
    if ref is None:
        raise ValueError(f"Could not fetch {uri} to find its type")
    return cfgs.make_qua(cfgs.canonicalize(uri), ref.type)


def enable_debug(reconciler):
    """Switch debug recording on in `reconciler`, its collector and every
    source reconciler, whatever the configuration says, and stop the
    collector storing what it fetches. The source
    reconcilers read the setting when they are built, so setting it on the
    configuration now would not reach them."""
    reconciler.debug = True
    reconciler.debug_graph = {}
    collector = reconciler.collector
    collector.debug = True
    collector.debug_graph = {}
    # a look, not a build: what the crawl fetches is not kept
    collector.acquirer_should_store = False
    for r in reconciler.reconcilers:
        r.debug = True
        r.debug_graph = {}


def replay(cfgs, reconciler, members):
    """Reconcile the records of one identity (`members`, the idmap's typed
    URIs for it) in roughly the order a build meets them, and return the
    reconciler's debug graph: ``{uri: [(uri, "eq"|"uri"|"nm"), ...]}``.
    A record that turns up among an earlier one's equivalents is not
    reconciled again."""
    inputs = []
    for u in members:
        if u.startswith("__"):
            continue  # the update token
        base = cfgs.split_qua(u)[0]
        split = cfgs.split_uri(base)
        if not split:
            logger.warning(f"{u}: no configured source matches it; skipped")
            continue
        src, ident = split
        rec = src["acquirer"].acquire(ident, store=False)
        if rec is None:
            logger.warning(f"{u}: could not acquire the record; skipped")
            continue
        inputs.append((src, base, rec))

    inputs.sort(key=lambda x: x[0]["datacache"].len_estimate())
    inputs.sort(key=lambda x: 1 if x[0]["type"] == "internal" else 2)

    while inputs:
        src, base, rec = inputs.pop(0)
        reconciler.reconcile(rec)
        found = [x["id"] for x in rec["data"].get("equivalent", [])]
        inputs = [i for i in inputs if not any(i[1].startswith(e) for e in found)]
    return reconciler.debug_graph


def node_labels(cfgs, graph):
    """``{uri: "<source>:<n>"}`` for every URI in the debug graph, numbered
    in the order they are met."""
    labels = {}

    def label(uri):
        if uri not in labels:
            split = cfgs.split_uri(uri)
            if split:
                name = split[0]["name"]
            else:
                name = EXTRA_NAMESPACES.get(uri.rsplit("/", 1)[0], "???")
            labels[uri] = f"{name}:{len(labels)}"
        return labels[uri]

    for k, targets in graph.items():
        label(k)
        for uri, _ in targets:
            label(uri)
    return labels


def graph_edges(graph, labels):
    """``[(from label, to label, kind), ...]`` without repeats, in order."""
    out, seen = [], set()
    for k, targets in graph.items():
        for uri, kind in targets:
            edge = (labels[k], labels[uri], kind)
            if edge[:2] not in seen:
                seen.add(edge[:2])
                out.append(edge)
    return out


def shortest_path(edges, start, end):
    """The labels from `start` to `end` along the edges taken either way,
    or ``None`` when they are not connected."""
    import networkx as nx

    g = nx.Graph()
    g.add_edges_from((a, b) for a, b, _ in edges)
    try:
        return nx.shortest_path(g, start, end)
    except (nx.NetworkXNoPath, nx.NodeNotFound):
        return None


def render(edges, labels, highlight=(), console=None):
    """The graph as a netext renderable."""
    from netext import ArrowTip, ConsoleGraph, EdgeSegmentDrawingMode
    from rich.style import Style

    nodes = {}
    for lbl in labels.values():
        colour = "bold cyan" if lbl in highlight else "magenta"
        nodes[lbl] = {"$shape": "box", "$style": Style.parse(colour)}
    data = {"$end-arrow-tip": ArrowTip.ARROW, "$style": Style(color="yellow"),
            "$edge-segment-drawing-mode": EdgeSegmentDrawingMode.BOX_ROUNDED}
    kwargs = {"console": console} if console is not None else {}
    return ConsoleGraph(nodes, [(a, b, dict(data, **{"$label": kind})) for a, b, kind in edges],
                        **kwargs)


class CommandHandler(BaseHandler):
    def add_args(self, ap):
        ap.add_argument("--recid", required=True,
                        help="Two URIs of the same identity, separated by a comma: from,to")

    def process(self, args, rest):
        super().process(args, rest)
        try:
            import netext  # noqa: F401
            import networkx  # noqa: F401
        except ImportError as e:
            raise RuntimeError(f"lux viz needs netext and networkx ({e.name} is missing): "
                               "pip install -e .[ui-extras]") from None
        from rich.console import Console

        from lux_pipeline.process.reconciler import Reconciler

        parts = [p.strip() for p in args.recid.split(",")]
        if len(parts) != 2 or not all(parts):
            raise ValueError("--recid takes two URIs separated by a comma: from,to")
        cfgs = self.configs
        qua_from, qua_to = (typed_uri(cfgs, p) for p in parts)

        idmap = cfgs.get_idmap()
        yuid, yuid2 = idmap[qua_from], idmap[qua_to]
        if yuid is None or yuid2 is None:
            missing = qua_from if yuid is None else qua_to
            raise ValueError(f"{missing} is not in the idmap")
        if yuid != yuid2:
            raise ValueError(f"{parts[0]} is {yuid} but {parts[1]} is {yuid2}: "
                             "they are not the same identity, so nothing connects them")

        networkmap = cfgs.instantiate_map("networkmap")["store"]
        reconciler = Reconciler(cfgs, idmap, networkmap)
        enable_debug(reconciler)
        graph = replay(cfgs, reconciler, idmap[yuid])
        if not graph:
            print(f"Reconciling the records of {yuid} recorded no connections")
            return None

        labels = node_labels(cfgs, graph)
        edges = graph_edges(graph, labels)
        ends = [labels.get(cfgs.split_qua(q)[0]) for q in (qua_from, qua_to)]
        console = Console()
        console.print(render(edges, labels, highlight=[e for e in ends if e], console=console))

        print(f"\n{yuid}: {len(labels)} URIs, {len(edges)} connections\n\nKey:")
        for uri, lbl in sorted(labels.items(), key=lambda x: int(x[1].rsplit(":", 1)[1])):
            print(f"  {lbl:<20}{uri}")
        if None in ends:
            print(f"\n{parts[0] if ends[0] is None else parts[1]} is not in the graph")
            return graph
        path = shortest_path(edges, *ends)
        print(f"\nShortest path from {parts[0]} to {parts[1]}:")
        print("  " + (" --> ".join(path) if path else "none: they are not connected in the graph"))
        return graph
