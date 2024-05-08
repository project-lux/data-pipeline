
class Collector(object):

    def __init__(self, config, idmap=None, networkmap=None):
        self.configs = config
        self.sources = list(config.external.values())
        self.internal_sources = list(config.internal.values())
        self.sources.extend( self.internal_sources )
        if idmap is None:
            if config.idmap_name != "":
                idmap = config.map_stores[config.idmap_name].get('store', None)
            if idmap is None:
                raise ValueError("Cleaner needs an idmap via idmap_name in config")
            else:
                self.idmap = idmap
        else:
            self.idmap = idmap
        self.networkmap = config.instantiate_map('networkmap')['store']        
        self.debug = config.debug_reconciliation
        self.global_reconciler = config.results['merged']['reconciler']

    def test_dates(self, botbr, botbx):
        if not botbr or not botbx:
            return False
        if botbr[0] == '-':
            botbr = botbr[1:]
        if botbx[0] == '-':
            botbx = botbx[1:]
        try:
            ry = int(botbr[:4])
            xy = int(botbx[:4])
            if abs(ry-xy) > 10:
                if self.debug: print(f"Failed as years are too far apart: {botbr} vs {botbx}")
                return False
        except:
            pass        
        return True


    def should_process_record(self, rec, xrec, equiv_recs):

        okay = True

        if rec['type'] == 'Person' and xrec['type'] == 'Person':
            if 'born' in rec and 'timespan' in rec['born'] and 'born' in xrec and 'timespan' in xrec['born']:
                # Test dates are similar
                botbr = rec['born']['timespan'].get('begin_of_the_begin', '')
                botbx = xrec['born']['timespan'].get('begin_of_the_begin', '')
                okay = self.test_dates(botbr, botbx)

            if okay and 'died' in rec and 'timespan' in rec['died'] and 'died' in xrec and 'timespan' in xrec['died']:
                botbr = rec['died']['timespan'].get('begin_of_the_begin', '')
                botbx = xrec['died']['timespan'].get('begin_of_the_begin', '')
                okay = self.test_dates(botbr, botbx)

        elif rec['type'] == 'Group' and xrec['type'] == 'Group':                        

            if 'formed_by' in rec and 'timespan' in rec['formed_by'] and 'formed_by' in xrec and 'timespan' in xrec['formed_by']:
                # Test dates are similar
                botbr = rec['formed_by']['timespan'].get('begin_of_the_begin', '')
                botbx = xrec['formed_by']['timespan'].get('begin_of_the_begin', '')
                okay = self.test_dates(botbr, botbx)

            if okay and 'dissolved_by' in rec and 'timespan' in rec['dissolved_by'] and 'dissolved_by' in xrec and 'timespan' in xrec['dissolved_by']:
                botbr = rec['dissolved_by']['timespan'].get('begin_of_the_begin', '')
                botbx = xrec['dissolved_by']['timespan'].get('begin_of_the_begin', '')
                okay = self.test_dates(botbr, botbx)

        elif rec['type'] == 'Place' and xrec['type'] == 'Place':
            # No good test?
            # polygon could start anywhere. Could test centroid?
            pass

        elif rec['type'] in ['MeasurementUnit', 'Currency', 'Language', 'Material'] and \
            xrec['type'] in ['MeasurementUnit', 'Currency', 'Language', 'Material'] and \
            xrec['type'] != rec['type']:
            okay = False

        elif rec['type'] == 'Place' and xrec['type'] != 'Place':
            # print("Not adding non-Place to Place")
            okay = False

        if okay:
            for qrec in equiv_recs.values():
                if 'part_of' in xrec and qrec['id'] in [x.get('id', '') for x in xrec['part_of']]:
                    okay = False
                if 'part_of' in qrec and xrec['id'] in [x.get('id', '') for x in qrec['part_of']]:
                    okay = False
                if 'broader' in xrec and qrec['id'] in [x.get('id', '') for x in xrec['broader']]:
                    okay = False
                if 'broader' in qrec and xrec['id'] in [x.get('id', '') for x in qrec['broader']]:
                    okay = False
                if 'member_of' in xrec and qrec['id'] in [x.get('id', '') for x in xrec['member_of']]:
                    okay = False
                if 'member_of' in qrec and xrec['id'] in [x.get('id', '') for x in qrec['member_of']]:
                    okay = False
                if not okay:
                    # print("Found cycle in part/broader/member, not adding")
                    pass
        return okay

    def collect(self, record):
        if 'data' in record:                
            rec = record['data']
        else:
            rec = record

        if 'equivalent' in rec:
            equiv = [x['id'] for x in rec['equivalent']]
        else:
            equiv = []
        if not equiv:
            # Nothing to do
            return record

        try:
            cls = rec['type'] 
        except:
            print(f"Record without a type: {record}")
            raise

        lbl = rec.get('_label', '')
        base = equiv[:]
        done = []
        yuid = rec.get('id', None)
        equiv_recs = {}

        # We're going to add them back in
        rec['equivalent'] = []

        # Reconciliation against indexes will have already been done
        # This function just crawls for records
        if self.debug: print(f"    Collecting {rec['id']}")
        while equiv:
            uri = equiv.pop(0)

            if self.debug: print(f"      ... collect processing {uri}")
            if uri in done:
                if self.debug: print(f"Skipping {uri} as done")
                continue

            try:
                source, identifier = self.configs.split_uri(uri)
            except TypeError:
                # returned None
                if self.debug: print(f"       couldn't split {uri} to a known source")
                if not uri in base:
                    done.append(uri)
                continue  

            canon = f"{source['namespace']}{identifier}"
            if canon != uri:
                # Throw out non canonical URIs
                uri = canon

            xrecord = source['acquirer'].acquire(identifier, cls)

            if xrecord:
                # Now we have the external record in xrec
                # 1. Test if we should filter it out as garbage
                xrec = xrecord['data']
                try:
                    okay = self.should_process_record(rec, xrec, equiv_recs)
                except:
                    print(f"Collect failed to decide if should process {identifier}...")
                    continue
                if not okay:
                    # Skip this one
                    if self.debug: print(f"       Not processing {xrec['id']} into {rec['id']}")                    
                    continue  # go to next equiv
                else:
                    # Good to add this one
                    try:
                        xrid = xrec['id']
                    except:
                        print(f"In collect, record has no id?! {xrec}")
                        continue
                    xrlbl = xrec.get('_label', lbl)
                    rec['equivalent'].append({"id": xrid, "type": cls, "_label": xrlbl})
                    equiv_recs[xrec['id']] = xrec
                    if self.debug: print(f"{xrid} / {xrlbl} into {rec['id']} tested okay")

                # 2. xrec is okay, so process *its* equivalents
                if 'equivalent' in xrec:
                    xeqs = xrec['equivalent']
                    xpfxs = {}
                    # This could do a better job determining the prefix part
                    # but would be Much Slower to iterate through all known vocabs
                    for xeq in xeqs:
                        xpfx = xeq['id'].rsplit('/', 1)[0]
                        try:
                            xpfxs[xpfx] += 1
                        except:
                            xpfxs[xpfx] = 1

                    for eq in xrec['equivalent']:
                        eqid = eq['id']
                        myxpfx = eqid.rsplit('/', 1)[0]
                        if xpfxs[myxpfx] > 2:
                            # Clearly they don't know what is happening so ignore them all
                            if self.debug: print(f"me: {myxpfx}\nxpfxs: {xpfxs}")
                            continue

                        known = self.configs.split_uri(eqid)                        
                        if known:
                            # Canonicalize
                            eqid = f"{known[0]['namespace']}{known[1]}"
                        currids = equiv_recs.keys()
                        if self.debug: print(f"     testing {eqid} from {xrec['id']}")

                        diffs = self.global_reconciler.reconcile(eqid, 'diffs')
                        if diffs is None:
                            diffs = set([])
                        if diffs.intersection(set(currids)):
                            # print(f"Found distinct, not merging {eqid} to {rec['id']}")
                            pass
                        elif not eqid in done and not eqid in base and not eqid in currids:
                            okay = False
                            if not known:
                                # Try to fix and match external uri
                                for bad, good in self.configs.external_uri_rewrites.items():
                                    if bad in eqid:
                                        eqid = eqid.replace(bad, good)
                                        break
                                for ext in self.configs.other_external_matches:
                                    if ext in eqid:
                                        okay = True
                                        break
                                # not okay if just http/https swap and not known
                                if eqid.startswith('http') and eqid.replace('http', 'https') in currids:
                                    okay = False
                                elif eqid.startswith('https') and eqid.replace('https', 'http') in currids:                                
                                    okay = False
                            else:
                                # Check if we should drop it out as already have an entry from this source
                                # Should this be before we start processing? e.g. does having one bad equivalent
                                # mean all the equivalents should be thrown out?
                                if known[0].get('uniquePerRecord', False):
                                    for xid in currids:
                                        if xid.startswith(known[0]['namespace']):
                                            # Just trash it
                                            if self.debug: print(f"Dropping {eqid} as already have {xid}")
                                            known = None
                                            break
                            if known or okay:
                                equiv.append(eqid)
                                if self.debug: print(f"       --> Adding {eqid} from {xrec['id']}")
                            else:
                                if self.debug: print(f"       --> NOT adding {eqid} from {xrec['id']}")
            done.append(uri)

        return record