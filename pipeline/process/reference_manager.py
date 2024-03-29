
import os
import time
import json

class ReferenceManager(object):

    def __init__(self, configs, idmap):
        self.configs = configs
        self.metatypes_seen = {}
        self.all_refs = configs.instantiate_map('all_refs')['store']
        self.done_refs = configs.instantiate_map('done_refs')['store']
        self.idmap = idmap
        self.debug = configs.debug_reconciliation

        self.internal_uris = [configs.internal_uri]
        for c in configs.internal.values():
            self.internal_uris.append(c['namespace'])

        # XXX FIXME: This should be a CSV or sane JSON
        fh = open(os.path.join(configs.data_dir,'replacements.json'))
        data = fh.read()
        fh.close()
        js = json.loads(data)
        getty_redirects = {}
        res = js['results']['bindings']
        for r in res:
            f = r['from']['value']
            t = r['to']['value']
            getty_redirects[f] = t
        self.redirects = getty_redirects


    def write_metatypes(self, my_slice):
        # write out our slice of metatypes
        if my_slice > -1:
            fn = f"metatypes-{my_slice}.json"
        else:
            fn = f"metatypes-single.json"
        fh = open(fn, 'w')
        fh.write(json.dumps(self.metatypes_seen))
        fh.close()        


    def pop_ref(self):
        return self.all_refs.popitem()

    def pop_done_ref(self):
        return self.done_refs.popitem()

    def did_ref(self, uri, distance):
        self.done_refs[uri] = {'dist': distance}

    def delete_done_ref(self, eq):  
        try:
            del self.done_refs[eq]        
        except:
            # didn't exist anyway
            pass

    # a ref is {'dist': int, 'type': str}
    def add_ref(self, ref, refs, distance, ctype): 
        xr = self.all_refs[ref]
        dref = self.done_refs[ref]
        if xr is not None:
            xdist = xr['dist']
            xctype = xr['type']
            # Test distance
            try:
                if xdist is None and dref is not None and dref['dist'] > distance:
                    del self.done_refs[ref]
                    self.all_refs[ref] = {'dist': distance, 'type': ctype}
                elif distance < xdist:
                    xr['dist'] = distance
            except:
                # Sometimes this tries to test None using >
                print(f" *** dref-dist {dref['dist']} > distance: {distance}")
                return None
            # Test concept type
            if not xctype and ctype:
                xr['type'] = ctype
        elif dref is not None:
            # Test Distance
            if distance is not None and dref['dist'] is not None and dref['dist'] > distance:
                # Add it back in
                del self.done_refs[ref]
                self.all_refs[ref] = {'dist': distance, 'type': ctype}
        elif not ref in refs:
            refs[ref] = {'dist': distance, 'type': ctype}

    def walk_for_refs(self, node, refs, distance, top=False):
        # Test if we need to record the node

        if not top and 'id' in node and not node['id'].startswith('_'):
            if node['id'] in self.redirects:
                node['id'] = self.redirects[node['id']]

            val = self.configs.make_qua(node['id'], node['type'])
            t = node.get('type', '')
            ct = t if t in self.configs.parent_record_types else ""
            self.add_ref(val, refs, distance, ct)

            # save meta-types
            if (node['type'] in self.configs.parent_record_types or node['type'] == 'Type') and 'classified_as' in node:
                cxids = [x['id'] for x in node['classified_as'] if 'id' in x]
                if not node['id'] in self.metatypes_seen:
                    self.metatypes_seen[node['id']] = []            
                for cx in cxids:
                    if not cx in self.metatypes_seen[node['id']]:
                        self.metatypes_seen[node['id']].append(cx)

        for (k,v) in node.items():
            if k in ['equivalent', 'access_point', 'conforms_to']:
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
        if 'data' in rec:
            rec = rec['data']
        if not 'id' in rec:
            return {}

        try:
            self.walk_for_refs(rec, refs, distance+1, top=True)
        except ValueError as e:        
            print(f"\nERROR: Walk error in {rec['id']}: {e}")
            # raise

        if 'equivalent' in rec:
            for eq in rec['equivalent']:
                k = self.configs.make_qua(eq['id'], rec['type'])
                t = rec.get('type', '')
                ct = t if t in self.configs.parent_record_types else ""
                self.add_ref(k, refs, distance, ct)

        topid = rec['id']
        ks = list(refs.keys())
        for k in ks:
            for i in self.internal_uris:
                if k.startswith(i) and topid.startswith(i):
                    del refs[k]
                    break

        self.all_refs.update(refs)
        return refs


    def manage_identifiers(self, rec, rebuild=False):
        if not rec or not 'data' in rec or not 'id' in rec['data']:
            return
        recid = rec['data']['id']
        typ = rec['data']['type']
        equivs = [x['id'] for x in rec['data'].get('equivalent', [])]
        qequivs = [self.configs.make_qua(x, typ) for x in equivs]

        # This should be called after ALL reconciliation processing has happened
        # including id->id, name->id and id collection to minimize duplicate records
        qrecid = self.configs.make_qua(recid, typ)
        equiv_map = {}
        existing = []
        if qrecid in self.idmap:
            # We know about this entity/record already
            uu = self.idmap[qrecid]
            if uu is None:
                print(f"got None? waiting and trying again...")
                time.sleep(1)
                uu = self.idmap[qrecid]
                if uu is None:
                    print(f"WTF... Still none... treating as ... None")
            if uu is not None:
                equiv_map[recid] = uu
                uuset = self.idmap[uu]
                if not uuset:
                    existing = []
                else:
                    existing = list(uuset)
        else:
            uu = None

        # Ensure that previous bad reconciliations are undone
        if uu and rebuild:
            has_update = self.idmap.has_update_token(uu)
            if not has_update:
                self.idmap.add_update_token(uu)
                if existing:
                    # replace existing with equivs if no or old update token
                    to_delete = []
                    for x in existing.copy():
                        if not x in qequivs:
                            if self.debug: print(f"Removing {x} not in new equivs")
                            existing.remove(x)
                            if not x.startswith("__"):
                                try:
                                    del self.idmap[x]
                                except:
                                    print(f"\nWhile processing {recid} found {equivs} in record")
                                    print(f"Tried to delete {x} for {uu}")
                        else:
                            if self.debug: print(f"Found {x} in existing and new")
          
        # Build map of equivalent ids given in current record
        if equivs:
            for eq in equivs.copy():
                qeq = self.configs.make_qua(eq, typ)
                if not qeq:
                    print(f"Made id {qeq} from {eq},{typ}?!")
                    continue
                if "'" in eq or ')' in eq:
                    print(f"\nERROR: *** Found bad character in {eq} from {recid}")
                    self.idmap._force_delete(qeq)
                    equivs.remove(eq)
                    continue
                if qeq not in existing:
                    myqeq = self.idmap[qeq]
                    if myqeq is not None:
                        equiv_map[eq] = myqeq

        # Ensure existing from idmap are in equivalent map
        # This will only make changes on second and subsequent times
        # we encounter the YUID
        if existing:
            for xq in existing.copy():
                if "'" in xq or ')' in xq:
                    print(f"\nERROR: *** Found bad character in {xq} from idmap:{uu}")
                    self.idmap._force_delete(self.configs.make_qua(xq, typ))
                    existing.remove(xq)
                    continue
                if not xq.startswith('__'):
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
            if self.debug: print(f"Minted {slug}/{uu} for {qrecid} ")

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
                if not recid in equiv_map:
                    # e.g. second occurence of Wiley painting
                    if qrecid is not None:
                        try:
                            self.idmap[qrecid] = uu
                        except:
                            print(f"Failed to set {qrecid} to {uu} from {equiv_map} / {uus}")
                            raise
                    else:
                        print(f"\nERROR: *** In manage_identifiers for {recid}, qrecid is None?!")
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
                            if src['type'] == 'internal':
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

                # Delete the others and set new uu
                for ud in uus:
                    existing_ud = self.idmap[ud]
                    if existing_ud:
                        for eqd in existing_ud:
                            if not eqd.startswith("__"):
                                self.idmap.delete(eqd) 
                                self.idmap[eqd] = uu

        # Ensure all equivs match to the yuid
        for eq in equiv_map.keys():
            if not eq.startswith("__") and not eq in existing:
                if not self.configs.is_qua(eq):
                    qeq = self.configs.make_qua(eq, typ)
                else:
                    qeq = eq
                self.idmap[qeq] = uu
     