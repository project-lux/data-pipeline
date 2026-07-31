class Reidentifier(object):
    # Prefetched {qua_key: yuid_or_None} for the record currently being
    # processed, populated by prefetch() at the top of each record tree.
    # Class-level defaults so partially-built instances (tests construct via
    # object.__new__) still work; batch is always rebound, never mutated.
    batch = {}
    # Count of lookups that missed the batch and had to go to the idmap. A
    # non-zero value means _node_keys has drifted out of step with
    # process_entity: still correct, but a round trip per miss.
    batch_misses = 0

    def __init__(self, configs, idmap):
        self.configs = configs
        self.ignore_ns = []
        self.collector = None
        self.reconciler = None
        for cfg in configs.internal.values():
            self.ignore_ns.append(cfg["namespace"])
        self.idmap = idmap
        self.debug = configs.debug_reconciliation

        self.do_not_reidentify = configs.do_not_reidentify
        self.ignore_props = ["access_point", "conforms_to"]
        self.use_slug = True
        self.equivalent_refs = True

        self.preserve_equivalents = {}
        for g in configs.globals:
            aat = configs.globals_cfg[g]
            uu = configs.globals[g]
            if aat[0] == "3":
                self.preserve_equivalents[uu] = f"http://vocab.getty.edu/aat/{aat}"
            elif aat[0] == "Q":
                self.preserve_equivalents[uu] = f"http://www.wikidata.org/entity/{aat}"
            elif aat.startswith("http"):
                self.preserve_equivalents[uu] = aat

        self.redirects = {}

    def should_process_uri(self, uri):
        if uri.startswith("_") or not uri:
            # Don't process bnodes
            return False
        return True

    def find_slug(self, record):
        slug = ""
        if self.use_slug:
            t = record.get("type", "")
            slug = self.configs.ok_record_types.get(t, "unknown")
        return slug

    ### Batched idmap lookups ###
    # process_entity used to issue one blocking idmap lookup per node, so a
    # record with ~100 references cost ~100 sequential round trips. Instead
    # walk the record first to collect every key it will need, resolve them
    # in one pipelined request, and have process_entity read the answers
    # from that batch. Two round trips per record instead of ~100.
    #
    # This is only sound because the idmap is read-only outside
    # run-identify.py -- see the note in process_entity. Do not reuse the
    # batch in a phase that mints.

    def _node_keys(self, node, qcls, top=False):
        """The idmap keys process_entity will look up for THIS node alone.

        Returns (keys, recurse). keys is empty wherever process_entity
        short-circuits before any lookup; recurse is False only in the one
        case where it returns None, which stops _reidentify descending.
        MUST stay in step with process_entity."""
        recid = node.get("id", "")
        if not recid:
            # bnode-with-equivalents (early return) or nothing to do: no
            # lookups either way, but _reidentify still descends
            return (), True
        for dnri in self.do_not_reidentify:
            if dnri in recid:
                return (), True
        redir = self.redirects.get(recid)
        if redir:
            recid = redir
        if not top or not qcls:
            qcls = node.get("type", None)
            if not qcls:
                # process_entity returns None
                return (), False
        try:
            keys = [self.configs.make_qua(recid, qcls)]
            for eq in node.get("equivalent", []):
                if type(eq) == dict and "id" in eq:
                    keys.append(self.configs.make_qua(eq["id"], qcls))
        except ValueError:
            # unknown type: let the real pass raise where it does today
            return (), True
        return keys, True

    def _collect_keys(self, node, qcls, keys, top=False):
        node_keys, recurse = self._node_keys(node, qcls, top)
        keys.update(node_keys)
        if not recurse:
            return keys
        # recurse exactly as _reidentify does
        for k, v in node.items():
            if k in ("id", "equivalent") or k in self.ignore_props:
                continue
            if type(v) == dict:
                v = [v]
            elif type(v) != list:
                continue
            for i in v:
                if type(i) == dict:
                    self._collect_keys(i, i.get("type", None), keys)
        return keys

    def prefetch(self, record, rectype=None):
        """Every idmap key this record needs, in two round trips. Returns
        {key: value_or_None}; {} means batching is off for this record and
        every lookup falls back to the live idmap."""
        mget = getattr(self.idmap, "get_multi", None)
        if mget is None:
            # memory IdMap, test stubs
            return {}
        try:
            keys = self._collect_keys(record, rectype, set(), top=True)
            if not keys:
                return {}
            batch = mget(keys)
            # The top-level branch of process_entity also reads the member
            # set of the resolved YUID. That key isn't known until the
            # strings above resolve, so it needs a second hop; mirror the
            # same min() the real pass uses to choose it.
            top_keys, _ = self._node_keys(record, rectype, top=True)
            uus = {batch[k] for k in top_keys if batch.get(k)}
            if uus:
                batch.update(mget([min(uus)]))
            return batch
        except Exception as e:
            print(f"reidentifier prefetch failed ({e}); using per-node lookups")
            return {}

    def _lookup(self, key):
        """Prefetched value if this record's batch has it, else a live
        lookup. A miss is correct but costs a round trip, so count it."""
        try:
            return self.batch[key]
        except KeyError:
            self.batch_misses += 1
            return self.idmap[key]

    def process_entity(self, record, qcls=None, top=False):
        result = {}
        recid = record.get("id", "")
        equivs = record.get("equivalent", [])
        uu = None
        qrecid = None

        if recid:
            # Don't rewrite some URIs like creativecommons
            for dnri in self.do_not_reidentify:
                if dnri in recid:
                    # Don't try to rewrite them
                    return {"id": recid}
            # pre-rewrite redirected uris
            try:
                redir = self.redirects[recid]
            except:
                redir = None
            if redir:
                recid = redir

            if not top or not qcls:
                qcls = record.get("type", None)
                if not qcls:
                    return None
            qrecid = self.configs.make_qua(recid, qcls)

            if not self.should_process_uri(recid):
                # do nothing for this one, but recurse down
                # strip explicit bnode identifiers (_:)
                if not recid.startswith("_:"):
                    result["id"] = recid
        elif equivs:
            # not recid, but yes equivalents
            # So a bnode for us, but one with external URIs.
            # just preserve the equivalents and move on
            return {"equivalent": equivs}

        if recid or equivs:
            # get equivalents and uri first for this
            equiv_map = {}
            if equivs:
                equivs = [q["id"] for q in equivs if "id" in q]
                uu = None
                for eq in equivs:
                    qeq = self.configs.make_qua(eq, qcls)
                    myqeq = self._lookup(qeq)
                    if myqeq is not None:
                        equiv_map[eq] = myqeq
                    else:
                        # print(f"{qeq} not in idmap, but in equivs of {recid}")
                        # ISNI, FAST, WC Entities etc
                        pass

            if recid:
                uu = self._lookup(qrecid)
                if uu is not None:
                    # We know about this entity/record already
                    # 2026-07-30 -- RS: WHY was this called twice??
                    # uu = self.idmap[qrecid]
                    equiv_map[recid] = uu

            if not equiv_map:
                # Don't know anything at all, ask for a new yuid??
                # This shouldn't happen if previous phases have worked
                print(f"\n!!! reidentifier couldn't find YUID for {recid} --> {equivs}")
                return result
            else:
                # We have something from the data
                # set.pop() picked an arbitrary YUID per process when more
                # than one was present; pick deterministically instead
                uus = set(equiv_map.values())
                uu = min(uus)
                uus.discard(uu)
                if len(uus):
                    # This also shouldn't happen
                    if self.debug:
                        print(f"Found more than one YUID for {recid} / {equivs}")
                elif not recid in equiv_map:
                    # recid not in idmap but its equivalents are: a gap in
                    # the identify phase. The idmap is read-only outside
                    # run-identify.py -- writing here from 24 racing merge
                    # workers would reintroduce nondeterministic identity
                    # mutations. Use the resolved YUID for output and log
                    # the gap durably so identify can be fixed instead.
                    print(f"IDMAP-GAP: {qrecid} resolved to {uu} only via "
                          f"equivalents; identify phase did not register it")

            # And set up the URI on the way out
            result["id"] = uu

            if top:
                all_equivs = self._lookup(uu)
                if not all_equivs:
                    print(f"\n!!! Found missing yuid: {uu} from: {recid} / {equivs}")
                    all_equivs = []
                    return result
                all_equivs = [self.configs.split_qua(x)[0] for x in all_equivs]
                all_equivs = [x for x in all_equivs if not x.startswith("__")]
                my_equivs = [x["id"] for x in record.get("equivalent", [])]
                if set(all_equivs) != set(my_equivs):
                    lbl = record.get("_label", "")
                    for eq in all_equivs:
                        if not eq in my_equivs:
                            try:
                                result["equivalent"].append({"id": eq, "type": record["type"], "_label": lbl})
                            except:
                                result["equivalent"] = [{"id": eq, "type": record["type"], "_label": lbl}]
                else:
                    result["equivalent"] = record.get("equivalent", [])
            elif recid and ("/aat/" in recid or uu in self.preserve_equivalents):
                # we're embedded reference, if external, put into equivalent
                # for now only process aat
                if uu in self.preserve_equivalents:
                    recid = self.preserve_equivalents[uu]

                result["equivalent"] = [
                    {"id": recid, "type": record["type"], "_label": record.get("_label", "External Reference")}
                ]

        return result

    def _reidentify(self, record, rectype=None, top=False):
        if top:
            # single entry point for a whole record tree: batch every idmap
            # lookup the walk below will need
            self.batch = self.prefetch(record, rectype)
        result = self.process_entity(record, rectype, top)
        if result is None:
            return result

        # and recurse to process other fields
        for k, v in record.items():
            if k in ["id", "equivalent"]:
                # already processed above
                continue
            elif not type(v) in [list, dict] or k in self.ignore_props:
                # copy across
                result[k] = v
            else:
                # recurse
                orig = type(v)
                if orig == dict:
                    v = [v]
                else:
                    result[k] = []
                for i in v:
                    if type(i) == dict:
                        nres = self._reidentify(i, i.get("type", None))
                        if nres:
                            if orig == dict:
                                result[k] = nres
                            else:
                                result[k].append(nres)
        return result

    ### API ###

    def reidentify(self, record, rectype=None):
        rec = record["data"]
        recid = rec.get("id", "")
        if not recid:
            raise ValueError("broken record structure, no id")
        if not rectype:
            rectype = rec["type"]

        try:
            res = self._reidentify(rec, rectype, top=True)
        except:
            print(f"Reidentifier Broke processing rec: {recid}")
            raise

        try:
            new_id = res["id"]
        except:
            print(f"Couldn't find YUID for record {recid}")
            return None
        uu = new_id[new_id.rfind("/") + 1 :]
        record2 = {
            "data": res,
            "yuid": uu,
            "identifier": record["identifier"],
            "record_time": record.get("record_time", ""),
            "change": record.get("change", ""),
            "source": record.get("source", ""),
        }
        return record2
