from .collector import Collector


class Reconciler(object):
    def __init__(self, config, idmap, networkmap):
        self.reconcilers = []
        self.debug = config.debug_reconciliation
        self.reconcileTypes = config.reconcile_record_types
        self.config = config

        for src in config.external.values():
            rlr = src.get("reconciler", None)
            if rlr:
                self.reconcilers.append(rlr)
        for src in config.results.values():
            rlr = src.get("reconciler", None)
            if rlr:
                self.reconcilers.append(rlr)

        self.global_reconciler = config.results["merged"].get("reconciler", None)
        self.collector = Collector(config, idmap, networkmap)
        try:
            self.min_equivs = config.reconcile_min_equivs
        except Exception:
            self.min_equivs = 3
        try:
            self.filter_internal = config.reconcile_filter_internal_equivs
        except Exception:
            self.filter_internal = False

        # source-rec-uri: [(added-rec-uri, 'eq|uri|nm')]
        self.debug_graph = {}

    def reconcile(self, record):
        if self.global_reconciler is None:
            self.global_reconciler = self.configs.results["merged"]["reconciler"]

        # We only process these types...
        if not record["data"]["type"] in self.reconcileTypes:
            return record

        # Inject the record's URI into equivalents at this point
        # in order to work through a consistent list
        me = {"id": record["data"]["id"], "type": record["data"]["type"]}
        if "equivalent" in record["data"]:
            record["data"]["equivalent"].append(me)
        else:
            record["data"]["equivalent"] = [me]

        if self.debug:
            for eq in record["data"]["equivalent"]:
                if eq["id"] != record["data"]["id"]:
                    try:
                        self.debug_graph[record["data"]["id"]].append((eq["id"], "eq"))
                    except Exception:
                        self.debug_graph[record["data"]["id"]] = [(eq["id"], "eq")]

        if self.debug:
            print(f"\n--- {record['data']['id']} ---")
        leq = len(record["data"].get("equivalent", []))
        try:
            if self.debug:
                print("    (uris)")
            self.reconcile_uris(record)
            leqs = record["data"].get("equivalent", [])
            if self.filter_internal:  ## FIXME: uhhh... huh????
                to_remove = []
                for e in leqs:
                    for i in self.config.internal.values():
                        if e["id"].startswith(i["namespace"]):
                            to_remove.append(e)
                for tr in to_remove:
                    leqs.remove(tr)

            nleq = len(leqs)
            if nleq == leq and nleq < self.min_equivs:
                # XXX -- this could be smarter perhaps?
                # Try and reconcile based on names
                if self.debug:
                    print("    (names)")
                self.call_reconcilers(record, reconcileType="name")
                n2leq = len(record["data"].get("equivalent", []))
                if n2leq > nleq:
                    if self.debug:
                        print("    (uris)")
                    self.reconcile_uris(record)
        except Exception:
            print(f"\nERROR --- reconciliation errored for {record['identifier']}")
            raise
        return record

    def reconcile_uris(self, record):
        r_equivs = 1
        cr_equivs = 2
        # Check distinct / sameAs now
        try:
            while r_equivs == 1 or (not cr_equivs.issubset(r_equivs)):
                self.call_reconcilers(record, reconcileType="uri")
                r_equivs = set([x["id"] for x in record["data"].get("equivalent", [])])
                if self.debug:
                    print(f"r_equivs: {r_equivs}")
                if cr_equivs == 2 or (not cr_equivs.issubset(r_equivs)):
                    if self.debug:
                        print("      (collecting)")
                    self.collector.collect(record)
                    cr_equivs = set(
                        [x["id"] for x in record["data"].get("equivalent", [])]
                    )
                    if self.debug:
                        print(f"cr_equivs: {cr_equivs}")
                    if self.debug:
                        lg = self.collector.debug_graph
                        for k, v in lg.items():
                            try:
                                self.debug_graph[k].extend(v)
                            except Exception:
                                self.debug_graph[k] = v
                        self.collector.debug_graph = {}

        except Exception as e:
            print(
                f"\nERROR: Reconciling broke for {record['source']}/{record['identifier']}: {e}"
            )
            raise
        return record

    def call_reconcilers(self, record, reconcileType="all"):
        ids = [x["id"] for x in record["data"].get("equivalent", [])]
        new_equivs = True

        # sameAs is just a reconciler
        for eq in ids:
            diffs = self.global_reconciler.reconcile(eq, "diffs")
            if diffs:
                for d in diffs:
                    if d in ids:
                        print(
                            f"UHOH... Found two distinct entities already in equivalents: {d} and {eq} in {record['data']['id']}"
                        )
                        # FIXME: Just trash d and hope it's the right one?
                        # Can we do any better?
                        ids.remove(d)

        # [<pipeline.sources.authorities.getty.reconciler.AatReconciler object at 0x7fc89991dbe0>,
        # <pipeline.sources.authorities.lc.reconciler.LcnafReconciler object at 0x7fc8985b7a00>,
        # <pipeline.sources.authorities.lc.reconciler.LcshReconciler object at 0x7fc89841a550>,
        # <pipeline.sources.authorities.getty.reconciler.UlanReconciler object at 0x7fc897c71e20>,
        # <pipeline.sources.authorities.oclc.reconciler.ViafReconciler object at 0x7fc897b69a00>,
        # <pipeline.sources.general.wikidata.reconciler.WdReconciler object at 0x7fc8916b6bb0>,
        # <pipeline.sources.lux.final.reconciler.GlobalReconciler object at 0x7fc8911dd610>]

        reconcilers = self.reconcilers
        # FIXME: Order reconcilers based on source of record if reconcileType == names
        while new_equivs:
            new_equivs = False
            for r in reconcilers:
                try:
                    # Might return a single entry (reconciled to single source)
                    # or a list (if the reconciler knows multiple sources,
                    #   or if there's more than one actual match to add from a single source)
                    newids = r.reconcile(record, reconcileType=reconcileType)
                    if self.debug and r.debug:
                        # fetch link-graph from reconciler
                        lg = r.debug_graph
                        for k, v in lg.items():
                            if k in self.debug_graph:
                                for vi in v:
                                    if not vi in self.debug_graph[k]:
                                        self.debug_graph[k].append(vi)
                            else:
                                self.debug_graph[k] = v
                        r.debug_graph = {}
                except Exception:
                    print(r)
                    raise
                if newids:
                    if not type(newids) in [list, set]:
                        newids = [newids]
                    for nid in newids:
                        if nid not in ids:
                            if self.debug:
                                print(
                                    f" --- reconciler {r} / {reconcileType} found {nid} for {record['data']['id']}"
                                )
                            # Test distinct to avoid adding bad
                            diffs = self.global_reconciler.reconcile(nid, "diffs")
                            okay_to_add = True
                            for d in diffs:
                                if d in ids:
                                    if self.debug:
                                        print(
                                            f"Found two distinct entities: {d} and {nid} in {record['data']['id']}"
                                        )
                                    okay_to_add = False
                            if okay_to_add:
                                ids.append(nid)

            if ids:
                t = record["data"]["type"]
                lbl = record["data"].get("_label", "")
                if not "equivalent" in record["data"]:
                    record["data"]["equivalent"] = []
                curr = [x["id"] for x in record["data"]["equivalent"]]
                for i in ids:
                    if not i in curr:
                        if self.debug:
                            print(f"Adding {i} to record")
                        record["data"]["equivalent"].append(
                            {"id": i, "type": t, "_label": lbl}
                        )
                        new_equivs = True
        return record
