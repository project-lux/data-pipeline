import os
import time
import ujson as json


class ReferenceManager(object):
    def __init__(self, configs, idmap):
        self.configs = configs
        self.metatypes_seen = {}
        self.all_refs = configs.instantiate_map("all_refs")["store"]
        self.done_refs = configs.instantiate_map("done_refs")["store"]
        self.idmap = idmap
        self.debug = configs.debug_reconciliation

        self.internal_uris = [configs.internal_uri]
        for c in configs.internal.values():
            self.internal_uris.append(c["namespace"])

        # XXX FIXME: This should be a CSV or sane JSON
        with open(os.path.join(configs.data_dir, "replacements.json")) as fh:
            data = fh.read()
        js = json.loads(data)
        getty_redirects = {}
        res = js["results"]["bindings"]
        for r in res:
            f = r["from"]["value"]
            t = r["to"]["value"]
            getty_redirects[f] = t
        self.redirects = getty_redirects
        self.ref_cache = {}

    def write_metatypes(self, my_slice):
        # write out our slice of metatypes to temp dir
        if my_slice > -1:
            fn = f"metatypes-{my_slice}.json"
        else:
            fn = f"metatypes-single.json"
        fn = os.path.join(self.configs.temp_dir, fn)
        with open(fn, "w") as fh:
            fh.write(json.dumps(self.metatypes_seen))

    def merge_metatypes(self):
        # Merge all the metatype slices together
        all_files = os.listdir(self.configs.temp_dir)
        mt = {}
        for f in all_files:
            if f.startswith("metatypes-") and f.endswith('.json'):
                fn = os.path.join(self.configs.temp_dir, f)
                with open(fn) as fh:
                    data = fh.read()
                js = json.loads(data)
                # Delete the file we've read
                os.remove(fn)
                if not mt:
                    mt = js
                else:
                    for k,v in js.items():
                        if not k in mt:
                            mt[k] = v
                        else:
                            for i in v:
                                if not i in mt[k]:
                                    mt[k].append(i)
        with open(os.path.join(self.configs.data_dir, 'metatypes.json'), 'w') as fh:
            outstr = json.dumps(mt)
            fh.write(outstr)


    def write_done_refs(self):
        # step through all entries in done_refs and write URI
        # to a file, if distance <= MAX_DISTANCE
        # Then clear the db to save memory
        maxd = self.configs.max_distance
        fn = os.path.join(self.configs.temp_dir, "reference_uris.txt")
        with open(fn, "w") as fh:
            x = 0
            for k in self.done_refs.iter_keys():
                x += 1
                if not x % 100000:
                    fh.flush()
                    print(x)
                if k['dist'] == None:
                    print("Got distance of 'None' from done_refs")
                    continue
                if k["dist"] <= maxd:
                    fh.write(f"{k['dist']}|{k.pkey}\n")
        self.done_refs.clear()

    def iter_done_refs(self, my_slice, max_slice):
        fn = os.path.join(self.configs.temp_dir, "reference_uris.txt")
        with open(fn, "r") as fh:
            if my_slice < 0 or max_slice < 0:
                # just read the whole file
                line = fh.readline()
                line = line.strip()
                line = line.split("|", 1)
                while line:
                    yield line
                    line = fh.readline()
                    line = line.strip()
                    if line:
                        line = line.split("|", 1)
            else:
                okay = True
                while okay:
                    uri = [fh.readline() for x in range(max_slice)][my_slice]
                    uri = uri.strip()
                    if not uri:
                        okay = False
                    else:
                        uri = uri.split("|", 1)
                        yield uri


    def get_len_refs(self):
        return len(self.all_refs)

    def pop_ref(self):
        return self.all_refs.popitem()

    def pop_done_ref(self):
        return self.done_refs.popitem()

    def did_ref(self, uri, distance):
        self.done_refs[uri] = {"dist": distance}

    def delete_done_ref(self, eq):
        try:
            del self.done_refs[eq]
        except:
            # didn't exist anyway
            pass

    # type is needed for Concepts, as the qua is Type but the type is Material (etc)
    # a ref is {'dist': int, 'type': str}
    def add_ref(self, ref, refs, distance, ctype):
        if ref in self.ref_cache:
            return None

        xr = self.all_refs[ref]
        if xr is not None:
            xdist = xr["dist"]
            xctype = xr["type"]
        else:
            xdist = None
            xctype = None
        dref = self.done_refs[ref]
        if dref is not None:
            ddist = dref["dist"]
        else:
            ddist = None

        if xr is not None:
            # Test distance: In all, and in done, but less distance
            if ddist is not None and ddist > distance:
                # need to re-add it to all with new distance
                del self.done_refs[ref]
                self.all_refs[ref] = {"dist": distance, "type": ctype}
            elif xdist is not None and distance < xdist:
                # in all, not in done, less distance: update in all
                xr["dist"] = distance
            if not xctype and ctype:
                xr["type"] = ctype
        elif dref is not None:
            # Test Distance
            if ddist is not None and ddist > distance:
                # Add it back in
                del self.done_refs[ref]
                self.all_refs[ref] = {"dist": distance, "type": ctype}
        elif not ref in refs:
            val = {"dist": distance, "type": ctype}
            refs[ref] = val
            self.all_refs[ref] = val
            if distance == 1 and "vocab.getty.edu/aat" in ref:
                self.ref_cache[ref] = distance

    def walk_for_refs(self, node, refs, distance, top=False):
        # Test if we need to record the node

        if not top and "id" in node and not node["id"].startswith("_"):
            if node["id"] in self.redirects:
                node["id"] = self.redirects[node["id"]]

            val = self.configs.make_qua(node["id"], node["type"])
            should_add_ref = True
            for i in self.internal_uris:
                if val.startswith(i):
                    # these will get built as 0 regardless
                    # so don't record refs to them
                    should_add_ref = False
                    break
            if should_add_ref:
                t = node.get("type", "")
                ct = t if t in self.configs.parent_record_types else ""
                self.add_ref(val, refs, distance, ct)

            # but still want to save meta-types
            if (node["type"] in self.configs.parent_record_types or node["type"] == "Type") and "classified_as" in node:
                cxids = [x["id"] for x in node["classified_as"] if "id" in x]
                if not node["id"] in self.metatypes_seen:
                    self.metatypes_seen[node["id"]] = []
                for cx in cxids:
                    if not cx in self.metatypes_seen[node["id"]]:
                        self.metatypes_seen[node["id"]].append(cx)

        for k, v in node.items():
            if k in ["equivalent", "access_point", "conforms_to"]:
                continue
            if type(v) == list:
                for vi in v:
                    if type(vi) == dict:
                        self.walk_for_refs(vi, refs, distance)
            elif type(v) == dict:
                self.walk_for_refs(v, refs, distance)

    def walk_top_for_refs(self, rec, distance):
        refs = {}
        if rec is None:
            return refs
        if "data" in rec:
            rec = rec["data"]
        if not "id" in rec:
            return {}

        try:
            self.walk_for_refs(rec, refs, distance + 1, top=True)
        except ValueError as e:
            print(f"\nERROR: Reference walk error in {rec['id']}: {e}")
            raise

        if "equivalent" in rec:
            for eq in rec["equivalent"]:
                k = self.configs.make_qua(eq["id"], rec["type"])
                should_add_ref = True
                for i in self.internal_uris:
                    if k.startswith(i):
                        # these will get built as 0 regardless
                        # so don't record refs to them
                        should_add_ref = False
                        break
                if should_add_ref:
                    t = rec.get("type", "")
                    ct = t if t in self.configs.parent_record_types else ""
                    self.add_ref(k, refs, distance, ct)

        return refs

    def manage_identifiers(self, rec):
        if not rec or not "data" in rec or not "id" in rec["data"]:
            return
        recid = rec["data"]["id"]
        typ = rec["data"]["type"]
        equivs = [x["id"] for x in rec["data"].get("equivalent", [])]
        qequivs = [self.configs.make_qua(x, typ) for x in equivs]

        # This should be called after ALL reconciliation processing has happened
        # including id->id, name->id and id collection to minimize duplicate records
        qrecid = self.configs.make_qua(recid, typ)
        qequivs.append(qrecid)

        equiv_map = {}
        existing = []

        uu = self.idmap[qrecid]
        if uu is not None:
            # We know about this entity/record already
            if self.debug:
                print(f"Found {uu} for {qrecid}")
            equiv_map[qrecid] = uu
            uuset = self.idmap[uu]
            if uuset:
                existing = list(uuset)
                if self.debug:
                    print(f"Found existing: {existing}")
        else:
            if self.debug:
                print(f"Got None for {qrecid}, will mint or find")

        updated_token = False
        # if we have the current update token, then we've already been touched
        # so rebuild from scratch is == has_update
        if uu is not None:
            has_update = self.idmap.has_update_token(uu)
        else:
            has_update = False
        rebuild = not has_update

        # Ensure that previous bad reconciliations are undone
        # But only the first time we see this uuid
        if uu and rebuild:
            if self.debug:
                print("No update token!")
            self.idmap.add_update_token(uu)
            updated_token = True
            if existing:
                # replace existing with equivs if no or old update token
                to_delete = []
                for x in existing.copy():
                    if not x in qequivs:
                        if self.debug:
                            print(f"Removing {x} not in new equivs")
                        existing.remove(x)
                        if not x.startswith("__"):
                            try:
                                del self.idmap[x]
                                if self.debug:
                                    print(f"deleted {x}")
                            except:
                                print(f"\nWhile processing {recid} found {equivs} in record")
                                print(f"Tried to delete {x} for {uu}")
                    else:
                        if self.debug:
                            print(f"Found {x} in existing and new")

        # Build map of equivalent ids given in current record
        if equivs:
            for eq in equivs.copy():
                qeq = self.configs.make_qua(eq, typ)
                if qeq not in existing:
                    myqeq = self.idmap[qeq]
                    if myqeq is not None:
                        equiv_map[eq] = myqeq
                    if self.debug:
                        print(f"qeq: {qeq} / {myqeq}")

        # Ensure existing from idmap are in equivalent map
        # This will only make changes on second and subsequent times
        # we encounter the YUID
        if existing:
            for xq in existing.copy():
                if not xq.startswith("__"):
                    equiv_map[xq] = uu

        # It is possible that equiv_map contains multiple YUIDS
        # And we will need to merge
        if not equiv_map:
            # Don't know anything at all, ask for a new yuid
            slug = self.configs.ok_record_types.get(typ, None)
            if not slug:
                # This will never resolve so raise an error
                raise ValueError(f"Unknown type: {typ} for generating slug")
            uu = self.idmap.mint(qrecid, slug)
            self.idmap.add_update_token(uu)
            updated_token = True
            if self.debug:
                print(f"Minted {slug}/{uu} for {qrecid} ")

            for eq in equivs:
                qeq = self.configs.make_qua(eq, typ)
                try:
                    self.idmap[qeq] = uu
                except:
                    print(f"\nERROR: Failed to set {qeq} as yuid: {uu} for {qrecid} having just minted it?")

        else:
            # We have something from the data and/or previous build
            uul = list(equiv_map.values())
            uus = set(uul)
            if len(uus) == 1:
                uu = uus.pop()
                if not updated_token:
                    self.idmap.add_update_token(uu)
                    updated_token = True
                if not qrecid in equiv_map:
                    # e.g. second occurence of Wiley painting
                    try:
                        if self.debug:
                            print(f"Setting {qrecid} to {uu} as uus=1")
                        self.idmap[qrecid] = uu
                    except:
                        print(f"Failed to set {qrecid} to {uu} from {equiv_map} / {uus}")
                        raise
            else:
                # Merge the yuids together
                print(f" --- Merging {uus}")

                # Pick internal then external, and within pick the one with the most references
                internals = []
                externals = []
                for u in uus:
                    ids = self.idmap[u]
                    if ids:
                        for i in ids:
                            try:
                                src, recid = self.configs.split_uri(i)
                            except:
                                continue
                            if src["type"] == "internal":
                                internals.append([u, uul.count(u)])
                            else:
                                externals.append([u, uul.count(u)])
                if internals:
                    internals.sort(key=lambda x: x[1], reverse=True)
                    uu = internals[0][0]
                    uus.remove(uu)
                elif externals:
                    externals.sort(key=lambda x: x[1], reverse=True)
                    uu = externals[0][0]
                    uus.remove(uu)
                else:
                    # ? Just pick one at random
                    uu = uus.pop()

                if not updated_token:
                    self.idmap.add_update_token(uu)
                    updated_token = True
                # Delete the others and set new uu
                for ud in uus:
                    existing_ud = self.idmap[ud]
                    if existing_ud:
                        for eqd in existing_ud:
                            if not eqd.startswith("__"):
                                try:
                                    self.idmap.delete(eqd)
                                except:
                                    print(f" Failed to delete {eqd} from idmap; ref_mgr")
                                try:
                                    self.idmap[eqd] = uu
                                except:
                                    print(f" Failed to set {eqd} to {uu} in idmap; ref_mgr")

        # Ensure we touch the token
        if not updated_token and not has_update:
            print(f"Fell through to final touch! {uu} in {qrecid}")
            self.idmap.add_update_token(uu)

        # Ensure all equivs match to the yuid
        for eq in equiv_map.keys():
            if not eq.startswith("__") and not eq in existing:
                if not self.configs.is_qua(eq):
                    qeq = self.configs.make_qua(eq, typ)
                else:
                    qeq = eq
                if self.debug:
                    print(f"Setting {qeq} to {uu} in idmap")
                try:
                    self.idmap[qeq] = uu
                except Exception as e:
                    print(f"Failed to set {qeq} to {uu}?: {e}")
            else:
                if self.debug:
                    print(f"Saw {eq} in existing, not setting")
