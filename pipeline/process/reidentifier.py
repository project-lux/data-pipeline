import os

import ujson as json


class Reidentifier(object):
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

    def process_entity(self, record, qcls=None, top=False):
        result = {}
        recid = record.get("id", "")
        equivs = record.get("equivalent", [])

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

        if recid or equivs:
            # get equivalents and uri first for this
            equiv_map = {}
            if equivs:
                equivs = [q["id"] for q in equivs if "id" in q]
                uu = None
                for eq in equivs:
                    qeq = self.configs.make_qua(eq, qcls)
                    myqeq = self.idmap[qeq]
                    if myqeq is not None:
                        equiv_map[eq] = myqeq
                    else:
                        # print(f"{qeq} not in idmap, but in equivs of {recid}")
                        # ISNI, FAST, WC Entities etc
                        pass

            if recid:
                uu = self.idmap[qrecid]
                if uu is not None:
                    # We know about this entity/record already
                    uu = self.idmap[qrecid]
                    equiv_map[recid] = uu

            if not equiv_map:
                # Don't know anything at all, ask for a new yuid??
                # This shouldn't happen if previous phases have worked
                print(f"\n!!! reidentifier couldn't find YUID for {recid} --> {equivs}")
                return result
            else:
                # We have something from the data
                uus = set(list(equiv_map.values()))
                uu = uus.pop()
                if len(uus):
                    # This also shouldn't happen
                    if self.debug:
                        print(f"Found more than one YUID for {recid} / {equivs}")
                elif not recid in equiv_map:
                    # If recid not in equiv map, then this is a new main id for the same entity
                    # Seems unlikely, but possible
                    if self.debug:
                        print(f"Found YUID only via equivs, not recid {recid} / {equivs}")
                    self.idmap[qrecid] = uu

            # And set on way out
            result["id"] = uu

            if top:
                all_equivs = self.idmap[uu]
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
