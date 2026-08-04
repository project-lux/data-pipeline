import os
import time
import ujson as json

# properties whose contents are never followed for references; a frozenset
# rather than a list literal rebuilt on every key of every node
SKIP_PROPS = frozenset(["equivalent", "access_point", "conforms_to"])


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
        # str.startswith takes a tuple and tests them all in one C call,
        # instead of a python loop per node
        self.internal_uris_t = tuple(self.internal_uris)

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

        # pop_ref() hands out one reference at a time, but claims them from
        # redis in batches -- popitem() cost four round trips per reference.
        # Keep the batch modest: references claimed but not yet processed
        # when a worker dies are dropped for this build, and that was already
        # true of the one reference popitem() held.
        self.ref_batch = 50
        self._ref_buffer = []

    def write_metatypes(self, my_slice):
        # write out our slice of metatypes
        if my_slice > -1:
            fn = f"metatypes-{my_slice}.json"
        else:
            fn = f"metatypes-single.json"
        # values are sets (membership testing them was a list scan per
        # classification per node); sorted on the way out so the file is
        # also stable between runs
        out = {k: sorted(v) for (k, v) in self.metatypes_seen.items()}
        with open(fn, "w") as fh:
            fh.write(json.dumps(out))

    @staticmethod
    def _batched(itr, size):
        batch = []
        for item in itr:
            batch.append(item)
            if len(batch) >= size:
                yield batch
                batch = []
        if batch:
            yield batch

    @staticmethod
    def _yuid_token(yuid):
        """Compact stand-in for a YUID, for the dedupe set in
        write_done_refs(). Tens of millions of these are held at once and a
        128 bit int is about a third of the memory the 36 character string
        costs. Anything that isn't uuid-shaped falls back to the string."""
        tail = yuid.rsplit("/", 1)[-1]
        try:
            return int(tail.replace("-", ""), 16)
        except ValueError:
            return yuid

    def write_done_refs(self):
        # step through all entries in done_refs and write URI
        # to a file, if distance <= MAX_DISTANCE
        # iter_items() fetches a chunk per round trip; iter_keys() handed back
        # a live reference into redis, and reading k['dist'] off it -- which
        # this loop did three times per key -- was a round trip each time.
        #
        # One line per YUID, not one per URI. Several external URIs in the
        # same identity cluster resolve to the same YUID, and iter_done_refs()
        # slices this file by line number, so those siblings land in different
        # merge workers. Each then builds the SAME merged record and upserts
        # the same rows in merged_ and every <source>_rewritten_record_cache --
        # duplicated work, and with commits deferred two workers hold those row
        # locks across a whole batch until postgres kills one with `deadlock
        # detected`. Resolving the YUID once here, in the single process that
        # writes the file, is what makes the merge workers key-disjoint.
        #
        # Which sibling URI survives doesn't affect the merged output: merge
        # only uses the line to look up the YUID, and picks the base record
        # from the whole cluster by PREF_ORDER.
        #
        # Format is `dist|yuid|uri`. The YUID is written out rather than left
        # for each merge worker to look up again -- 24 workers resolving the
        # same reference is 24 redis round trips for one answer -- and it
        # makes the file self-describing, so a merge run against a file left
        # over from before the dedupe fails immediately and loudly instead of
        # deadlocking an hour in. The yuid field is empty for references the
        # idmap doesn't know.
        maxd = self.configs.max_distance
        seen = set()
        x = kept = deduped = unresolved = reported = 0
        with open("reference_uris.txt", "w") as fh:
            for chunk in self._batched(self.done_refs.iter_items(), 10000):
                wanted = []
                for (key, vals) in chunk:
                    x += 1
                    dist = vals.get("dist")
                    if dist is None:
                        print("Got distance of 'None' from done_refs")
                        continue
                    if dist <= maxd:
                        wanted.append((key, dist))
                if x - reported >= 100000:
                    reported = x
                    fh.flush()
                    print(x)
                if not wanted:
                    continue
                # get_multi rejects unqua'd keys; those can't be looked up at
                # all, so treat them as unresolved rather than failing the run
                yuids = self.idmap.get_multi([k for (k, d) in wanted if self.configs.is_qua(k)])
                for (key, dist) in wanted:
                    yuid = yuids.get(key)
                    if yuid is None:
                        # No YUID: nothing to collide with, and run-merge
                        # reports it. Keep the line so the gap stays visible.
                        unresolved += 1
                    else:
                        token = self._yuid_token(yuid)
                        if token in seen:
                            deduped += 1
                            continue
                        seen.add(token)
                    kept += 1
                    fh.write(f"{dist}|{yuid or ''}|{key}\n")
        print(f"reference_uris.txt: {kept} lines from {x} done refs "
              f"({deduped} duplicate YUIDs dropped, {unresolved} with no YUID)")

    @staticmethod
    def _split_done_ref(line):
        """`dist|yuid|uri` -> (dist, yuid, uri).

        A two-field line is the pre-dedupe format, which has one line per URI
        instead of per YUID -- so sibling URIs in one cluster go to different
        merge workers, which then upsert the same rows and deadlock. That is
        not something to limp along with, so say what to do about it."""
        parts = line.split("|", 2)
        if len(parts) != 3:
            raise ValueError(
                f"reference_uris.txt is in the old dist|uri format ({line!r}). "
                f"It is stale: regenerate it with "
                f"`python ./manage-data.py --write-refs` before merging, or "
                f"parallel merge workers will contend for the same rows.")
        return parts

    def iter_done_refs(self, my_slice, max_slice):
        with open("reference_uris.txt", "r") as fh:
            if my_slice < 0 or max_slice < 0:
                # just read the whole file
                # (previously an empty/blank first line yielded a bogus [""]
                # item because "".split("|") is a truthy [""])
                line = fh.readline()
                while line:
                    stripped = line.strip()
                    if stripped:
                        yield self._split_done_ref(stripped)
                    line = fh.readline()
            else:
                okay = True
                while okay:
                    uri = [fh.readline() for x in range(max_slice)][my_slice]
                    uri = uri.strip()
                    if not uri:
                        okay = False
                    else:
                        yield self._split_done_ref(uri)

    def pop_ref(self):
        if not self._ref_buffer:
            self._ref_buffer = self.all_refs.popitems(self.ref_batch)
            if not self._ref_buffer:
                return None
        return self._ref_buffer.pop()

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

    def collect_ref(self, ref, pending, distance, ctype):
        """Record a reference for the batched resolve at the end of the
        record. Pure bookkeeping -- no redis. Keeps the shortest distance
        seen, and the first non-empty ctype (matching the HSETNX the merge
        does, where the first writer sets the type)."""
        if ref in self.ref_cache:
            return None
        cur = pending.get(ref)
        if cur is None:
            pending[ref] = [distance, ctype]
        else:
            if distance < cur[0]:
                cur[0] = distance
            if not cur[1] and ctype:
                cur[1] = ctype

    def resolve_refs(self, pending):
        """Resolve one record's collected references in four round trips --
        one pipelined HGETALL against each map, one pipelined merge, one
        pipelined delete -- instead of the 7 to 11 per reference that the
        per-reference EXISTS/HGET/WATCH-MULTI path cost.

        Returns the refs newly added to all_refs, as add_ref always did."""
        refs = {}
        if not pending:
            return refs

        keys = list(pending)
        xrs = self.all_refs.get_multi(keys)
        drefs = self.done_refs.get_multi(keys)

        to_merge = []
        to_undone = []
        for ref in keys:
            distance, ctype = pending[ref]
            xr = xrs.get(ref)
            dref = drefs.get(ref)
            ddist = dref.get("dist") if dref is not None else None

            if xr is not None:
                # In all, and in done at a greater distance: re-add to all
                # with the new distance. Re-add BEFORE removing from done: a
                # crash between the two duplicates work rather than losing
                # the reference, so the merge batch is executed first below.
                to_merge.append((ref, distance, ctype))
                if ddist is not None and ddist > distance:
                    to_undone.append(ref)
            elif dref is not None:
                if ddist is not None and ddist > distance:
                    to_merge.append((ref, distance, ctype))
                    to_undone.append(ref)
            else:
                refs[ref] = {"dist": distance, "type": ctype}
                to_merge.append((ref, distance, ctype))
                if distance == 1 and "vocab.getty.edu/aat" in ref:
                    self.ref_cache[ref] = distance

        if to_merge:
            self.all_refs.merge_refs(to_merge)
        if to_undone:
            self.done_refs.delete_multi(to_undone)
        return refs

    def add_ref(self, ref, refs, distance, ctype):
        """Collect and resolve a single reference immediately. The record
        walk uses collect_ref/resolve_refs so a whole record's references
        cost four round trips rather than four per reference."""
        if ref in refs:
            return None
        refs.update(self.resolve_refs({ref: [distance, ctype]}))

    def walk_for_refs(self, node, pending, distance, top=False):
        # Test if we need to record the node

        if not top and "id" in node and not node["id"].startswith("_"):
            if node["id"] in self.redirects:
                node["id"] = self.redirects[node["id"]]

            val = self.configs.make_qua(node["id"], node["type"])
            # internal ones get built at distance 0 regardless, so aren't
            # recorded as references
            if not val.startswith(self.internal_uris_t):
                t = node.get("type", "")
                ct = t if t in self.configs.parent_record_types else ""
                self.collect_ref(val, pending, distance, ct)

            # but still want to save meta-types
            if (node["type"] in self.configs.parent_record_types or node["type"] == "Type") and "classified_as" in node:
                seen = self.metatypes_seen.get(node["id"])
                if seen is None:
                    seen = self.metatypes_seen[node["id"]] = set()
                for x in node["classified_as"]:
                    if "id" in x:
                        seen.add(x["id"])

        for k, v in node.items():
            if k in SKIP_PROPS:
                continue
            if type(v) is list:
                for vi in v:
                    if type(vi) is dict:
                        self.walk_for_refs(vi, pending, distance)
            elif type(v) is dict:
                self.walk_for_refs(v, pending, distance)

    def walk_top_for_refs(self, rec, distance):
        if rec is None:
            return {}
        if "data" in rec:
            rec = rec["data"]
        if not "id" in rec:
            return {}

        # Collect the whole record's references first, then hit redis once
        # for all of them: this walk used to issue 7-11 round trips per
        # reference node inline.
        pending = {}
        try:
            self.walk_for_refs(rec, pending, distance + 1, top=True)
        except ValueError as e:
            print(f"\nERROR: Reference walk error in {rec['id']}: {e}")
            raise

        if "equivalent" in rec:
            for eq in rec["equivalent"]:
                k = self.configs.make_qua(eq["id"], rec["type"])
                if not k.startswith(self.internal_uris_t):
                    t = rec.get("type", "")
                    ct = t if t in self.configs.parent_record_types else ""
                    # note: equivalents are recorded at `distance`, the walk
                    # above at `distance + 1`; collect_ref keeps the smaller
                    self.collect_ref(k, pending, distance, ct)

        return self.resolve_refs(pending)

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
