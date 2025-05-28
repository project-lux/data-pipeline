from argparse import ArgumentParser
from ._rich import Live, get_layout
from lux_pipeline.process.reconciler import Reconciler
import sys
import networkx as nx
from netext import ConsoleGraph, EdgeSegmentDrawingMode, EdgeRoutingMode, ArrowTip, AutoZoom
from netext.layout_engines import ForceDirectedLayout

from rich.color import Color
from rich.panel import Panel
from rich.style import Style
from rich import print

from ._handler import BaseHandler as BH


class CommandHandler(BH):
    def process(self, args, rest):
        ap = ArgumentParser()
        ap.add_argument("--recid", type=str, help="record id to process")
        ap.parse_args(rest, namespace=args)
        cfgs = self.configs

        # --recid in,out
        try:
            from_p, to_p = args.recid.split(",")
        except:
            print("Usage: lux viz --recid from_id,to_id")
            sys.exit(0)

        try:
            (src, ident) = cfgs.split_uri(from_p)
        except:
            print(f"Unknown URI: {from_p}")
            sys.exit()

        if "##qua" in from_p:
            qua = from_p
        else:
            try:
                ref = src["mapper"].get_reference(ident)
                base = cfgs.canonicalize(from_p)
                qua = cfgs.make_qua(base, ref.type)
            except:
                print(f"Could not make typed URI for {from_p}")
                raise
                sys.exit()

        if "##qua" in to_p:
            qua2 = to_p
        else:
            try:
                ref = src["mapper"].get_reference(ident)
                base = cfgs.canonicalize(to_p)
                qua2 = cfgs.make_qua(base, ref.type)
            except:
                print(f"Could not make typed URI for {to_p}")
                raise
                sys.exit()

        idmap = cfgs.get_idmap()
        networkmap = cfgs.instantiate_map("networkmap")["store"]
        yuid = idmap[qua]
        yuid2 = idmap[qua2]

        if yuid != yuid2:
            print(f"{from_p} is {yuid} but {to_p} is {yuid2}")
            print("No way to connect these two URIs")
            sys.exit()

        # --- set up environment ---
        reconciler = Reconciler(cfgs, idmap, networkmap)
        uris = idmap[yuid]

        ### Idea: We know the final result, try to reconstruct it by following what the reconciler
        # does, but tracking which URIs prompted the inclusion of which others
        # Should do it in the approximately same order as

        inputs = []
        for u in uris:
            if u.startswith("__"):
                continue
            (base, qua) = cfgs.split_qua(u)
            (src, ident) = cfgs.split_uri(base)
            inputs.append((src, ident, u, src["acquirer"].acquire(ident)))

        inputs.sort(key=lambda x: x[0]["datacache"].len_estimate())
        inputs.sort(key=lambda x: 1 if x[0]["type"] == "internal" else 2)

        reconciler.debug = True
        processed = []
        while inputs:
            inp = inputs.pop(0)
            reconciler.reconcile(inp[3])
            eqs = inp[3]["data"].get("equivalent", [])
            eqs = [x["id"] for x in eqs]
            for e in eqs:
                for i in inputs[:]:
                    if i[2].startswith(e):
                        inputs.remove(i)
                        break
            processed.append(inp)

        # We've now built reconciler.debug_graph
        # turn it into a networkx graph with shorter node ids

        curr_id = "0"
        idents = {}
        edge_labels = {}
        extra_nss = {"http://id.worldcat.org/fast": "fast"}
        G = nx.DiGraph()

        for k, v in reconciler.debug_graph.items():
            try:
                kl = idents[k]
            except:
                (src, ident) = cfgs.split_uri(k)
                kl = f"{src['name']}:{curr_id}"
                idents[k] = kl
                curr_id = chr(ord(curr_id) + 1)
            G.add_node(kl, label=kl)

            for vi in v:
                try:
                    vil = idents[vi[0]]
                except:
                    try:
                        (src, ident) = cfgs.split_uri(vi[0])
                        vil = f"{src['name']}:{curr_id}"
                    except:
                        # Could be fast or could be junk
                        (ns, idt) = vi[0].rsplit("/", 1)
                        if ns in extra_nss:
                            vil = f"{extra_nss[ns]}:{curr_id}"
                        else:
                            vil = f"???:{curr_id}"
                    idents[vi[0]] = vil
                    curr_id = chr(ord(curr_id) + 1)
                G.add_edge(kl, vil)
                edge_labels[(kl, vil)] = vi[1]

        nx.set_edge_attributes(G, ArrowTip.ARROW, "$end-arrow-tip")
        # nx.set_edge_attributes(G, ArrowTip.NONE, "$end-arrow-tip-0")
        nx.set_edge_attributes(G, lambda zoom: 0 if zoom < 0.5 else 1, "$lod-map")
        nx.set_edge_attributes(G, Style(color=Color.from_rgb(red=20, green=80, blue=10), bold=True), "$style")
        nx.set_edge_attributes(G, EdgeRoutingMode.STRAIGHT, "$edge-routing-mode")
        nx.set_edge_attributes(G, EdgeSegmentDrawingMode.BOX_ROUNDED, "$edge-segment-drawing-mode")
        nx.set_edge_attributes(G, Style(color="yellow", bold=True), "$style")

        nx.set_node_attributes(G, Style(color="magenta"), "$style")
        nx.set_node_attributes(G, lambda zoom: 0 if zoom < 0.5 else 1, "$lod-map")
        nx.set_node_attributes(G, "box", "$shape")

        layout = get_layout(cfgs, 1, args.log)
        with Live(layout, screen=False, refresh_per_second=4):
            cg = ConsoleGraph(G, layout_engine=ForceDirectedLayout(), zoom=(AutoZoom.FIT))
            r_panel = Panel(cg)
            layout["progress"].update(r_panel)
            # console = Console()
            # console.print(cg)

            if False:
                key = []
                inv_ident = {}
                for k, v in idents.items():
                    key.append((v, k))
                    inv_ident[v] = k
                key.sort()
                print("  -- Key --")
                for k in key:
                    print(f"  {k[0]:<16}{k[1]}")
                print("\nConnected Nodes:")
                for sets in list(nx.connected_components(G)):
                    print(sets)
                print(f"\nShortest path from {from_p} to {to_p}")
                print(" --> ".join([inv_ident[x] for x in nx.shortest_path(G, idents[from_p], idents[to_p])]))

                # plt.figure(figsize=(12, 12))
                uri_colors = {
                    "id.loc.gov": "#7CFC00",
                    "vocab.getty.edu": "#FFA07A",
                    "wikidata.org": "#FF1493",
                    "yale.edu": "#00BFFF",
                    "viaf.org": "#FFD700",
                }
                node_color_values = ["skyblue" for _ in G.nodes()]
                node_labels = {node: node for node in G.nodes()}
                edge_color_values = ["black" for _ in G.edges()]
                pos = nx.spring_layout(G, k=1)
                for lbl, n in idents.items():
                    col = "#E0E0FF"
                    for k, v in uri_colors.items():
                        if k in lbl:
                            col = uri_colors[k]
                            break
                    nx.draw_networkx_nodes(G, pos, nodelist=[n], node_color=[col], node_size=250, label=f"{n}: {lbl}")
                edges = nx.draw_networkx_edges(G, pos, edge_color=edge_color_values)
                nx.draw_networkx_labels(G, pos, labels=node_labels, font_size=14)
                nx.draw_networkx_edge_labels(G, pos, edge_labels=edge_labels, font_color="red")
