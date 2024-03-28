
import json
from shapely import wkt
from shapely.geometry import shape
from datetime import datetime, timedelta
from pipeline.process.reidentifier import Reidentifier


class MergeHandler(object):

    def __init__(self, config, idmap, ref_mgr=None):
        self.config = config
        self.idmap = idmap
        self.merger = RecordMerger(config, idmap)
        self.reference_manager = ref_mgr
        self.reidentifier = Reidentifier(config, idmap)

    def merge(self, record, to_merge):

        record['sources'] = [record['source']]

        to_do = []
        for eq in to_merge:
            if eq.startswith('__'):
                # This is the idmap's update token
                continue

            # Delete via Reference Manager
            self.reference_manager.delete_done_ref(eq)

            try:
                (ext_src, ident) = self.config.split_uri(eq)
            except:
                print(f" --- Failed to split uri: {eq}")
                continue
            if ext_src['type'] == "internal":
                ident = ident.split("#")[0]
            ext_rec = ext_src['recordcache'][ident]
            if not ext_rec:
                print(f"Reference to non-existent record {ident} in {ext_src['name']}")
                continue
            to_do.append((ext_src, ext_rec))


        # Now Merge
        to_do.sort(key=lambda x: x[0]['merge_order'])
        for (ext_src, ext_rec) in to_do:
            if ext_rec:
                ext_rec2 = self.reidentifier.reidentify(ext_rec, record['data']['type'])
                if ext_rec2:
                    ext_src['recordcache2'][ext_rec2['yuid']] = ext_rec2['data']
                    try:
                        # This returns record, but it's mutated directly so no need to assign it
                        self.merger.merge(record, ext_rec2)
                    except:
                        raise
                        try:
                            print(f"****** Failed to merge {record['data']['id']}")
                        except:
                            print(f"****** Failed to merge {to_do}")

        return record


class RecordMerger(object):

    def __init__(self, configs, idmap):
        # Alias class dependent functions
        self.merge_event = self.merge_activity
        self.merge_period = self.merge_activity        
        self.merge_birth = self.merge_activity
        self.merge_death = self.merge_activity
        self.merge_formation = self.merge_activity
        self.merge_dissolution = self.merge_activity
        self.merge_production = self.merge_activity
        self.merge_creation = self.merge_activity
        self.merge_currency = self.merge_type
        self.merge_material  = self.merge_type
        self.merge_language = self.merge_type
        self.merge_measurementunit = self.merge_type

        self.configs = configs
        self.idmap = idmap
        self.globals = configs.globals
        self.internal_sources = list(configs.internal.keys())


    def merge_type(self, rec, merge, msource, skip):
        ### This is LUX specific to manage LC types
        if 'created_by' in merge and not 'created_by' in skip:
            if 'influenced_by' in merge['created_by']:
                mc = merge['created_by']['influenced_by']
                rc = rec['created_by']['influenced_by']
                if len(mc) != len(rc):
                    print(f"In {rec['id']} - Strange creation influences - different counts!")
                    print(f"    {mc}\n    {rc}")
                else:
                    infs = []
                    for i in range(len(mc)):
                        if mc[i] != rc[i]:
                            if not 'id' in mc[i] or not 'id' in rc[i]:
                                # uh-oh!
                                print(f"Missing URI for concept components in {rec.get('id', '-')} / {merge.get('id', '-')}")
                            elif mc[i]['id'] == rc[i]['id']:
                                # Label is thus difference - select uppercased version
                                if mc[i]['_label'][0].isupper():
                                    mc[i]['_label'] = mc[i]['_label'].strip()
                                    infs.append(mc[i])
                                else:
                                    rc[i]['_label'] = rc[i]['_label'].strip()
                                    infs.append(rc[i])                              
                            elif mc[i]['_label'] == rc[i]['_label'] and mc[i]['type'] != rc[i]['type']:
                                # Label can be the same with different class
                                # select more specific over Type
                                if mc[i]['type'] == 'Type' and rc[i]['type'] != 'Type':
                                    infs.append(rc[i])
                                elif rc[i]['type'] == 'Type' and mc[i]['type'] != 'Type':
                                    infs.append(mc[i])
                                else:
                                    #print(f" {rec['id']} Df type: {mc[i]}\n          {rc[i]}")
                                    # Arbitrarily keep the largest record definition
                                    # XXX This should try to check consensus upstream:
                                    #    if there's 100 records for Person, and 1 for Group, then use Person
                                    #    but that means a second sweep over all the data
                                    infs.append(rc[i])
                            else:
                                # ???
                                # print(f" {rec['id']} ???????: {mc[i]}\n          {rc[i]}")
                                pass

                        else:
                            infs.append(rc[i])
                    rec['created_by']['influenced_by'] = infs
            else:
                print("--- merge of Type creation needs work")
                # One example -- sets created by a group that isn't apparently reconciled
                # for now just keep the record

        if 'broader' in merge and not 'broader' in skip:
            # Allow multiple broaders, from different hierarchies
            # will have re-identified the URIs by now
            ids = [x.get('id', None) for x in rec['broader'] if x]
            ids.append(rec['id'])
            for i in merge['broader']:
                iid = i.get('id', None)
                if iid and not iid in ids:
                    rec['broader'].append(i)
                    ids.append(i['id'])

    def merge_place(self, rec, merge, msource, skip):
        if 'defined_by' in merge and not 'defined_by' in skip:
            # FIXME do something sensible here
            # Not sure how to merge if different? Just ignore?
            if rec['defined_by'] != merge['defined_by']:

                a = rec['defined_by'].strip()
                b = merge['defined_by'].strip()
                shapes = []
                for x in [a,b]:
                    if x.startswith('{') and x.endswith('}'):
                        geo = json.loads(x)
                        if 'type' in geo and geo['type'] == 'FeatureCollection' and 'features' in geo:
                            ftrs = geo['features']
                            if len(ftrs) == 1:
                                ftr = ftrs[0]
                            else:
                                # Multiple features in the collection?
                                print(f"Found multiple features: {ftrs}")
                                ftr = ftrs[0]
                            shapes.append(shape(ftr['geometry']))
                    elif x.startswith('POINT') or x.startswith('POLYGON'):
                        shapes.append(wkt.loads(x))
                    else:
                        print(f"Unknown place.defined_by value: {x}")
                        shapes.append(None)

                awkt = shapes[0].wkt if shapes[0] else None
                bwkt = shapes[1].wkt if shapes[1] else None

                if not shapes[0] and bwkt is not None:
                    # replace with parseable!
                    rec['defined_by'] = bwkt
                elif awkt is not None and bwkt is None:
                    rec['defined_by'] = awkt
                elif awkt is None and bwkt is None:
                    # uhh oh oh...
                    del rec['defined_by']
                elif awkt.startswith('POINT') and bwkt.startswith('POLYGON'):
                    # replace a point with a polygon
                    rec['defined_by'] = bwkt
                elif awkt.startswith('POLYGON') and bwkt.startswith('POLYGON'):
                    lena = len(shapes[0].exterior.coords)
                    lenb = len(shapes[1].exterior.coords)
                    if lenb > lena:
                        # polygon rather than box
                        rec['defined_by'] = bwkt
                    else:
                        # Check precision of coords?
                        p1a = shapes[0].exterior.coords[0]
                        p1b = shapes[1].exterior.coords[0]
                        # assuming that the non decimal is the same, just compare str len
                        if len(str(p1b[0])) > len(str(p1a[0])) or len(str(p1b[1])) > len(str(p1a[1])):
                            rec['defined_by'] = bwkt

                elif awkt.startswith('POINT') and bwkt.startswith('POINT'):
                    p1a = shapes[0].coords[0]
                    p1b = shapes[1].coords[0]
                    if len(str(p1b[0])) > len(str(p1a[0])) or len(str(p1b[1])) > len(str(p1a[1])):
                        rec['defined_by'] = bwkt
                elif awkt.startswith('POLYGON') and bwkt.startswith('POINT'):
                    # Nope
                    pass
                else:
                    print(f"Failed to compare {awkt} and {bwkt}")

        for rp in ['part_of', 'approximated_by']:
            if rp in merge and not rp in skip:
                # Assume that reconciliation has worked, so same related entities have same id.
                ids = [x.get('id', None) for x in rec[rp] if x]
                ids.append(rec['id'])
                for i in merge[rp]:
                    if 'id' in i and i['id'] and not i['id'] in ids:
                        rec[rp].append(i)
                        ids.append(i['id'])                



    def merge_actor(self, rec, merge, msource, skip):
        if 'carried_out' in merge and not 'carried_out' in skip:
            # These carried_outs should only be professional activities according to the spec,
            # otherwise they're somewhere else in the model on the affected entity, rather than
            # on the actor
            amerge = None
            arec = None
            for a in merge['carried_out']:
                ids = [x.get('id', None) for x in a['classified_as'] if x]
                if 'http://vocab.getty.edu/aat/300393177' in ids:
                    amerge = a
                    break
            for b in rec['carried_out']:
                ids = [x.get('id', None) for x in b['classified_as'] if x]     
                if 'http://vocab.getty.edu/aat/300393177' in ids:
                    arec = b
                    break                
            if amerge and arec:
                self.merge_common(arec, amerge, msource)

        if 'residence' in merge and not 'residence' in skip:
            # Can live in two places. Trust reconciliation to have already merged identical
            ids = [x.get('id',None) for x in rec['residence'] if x]
            for i in merge['residence']:
                iid = i.get('id', None)
                if iid and not iid in ids:
                    rec['residence'].append(i)
                    ids.append(iid)                

        if 'contact_point' in merge and not 'contact_point' in skip:
            # Can have diff contacts of same type (two email addresses)
            # contacts are Identifiers, so merge on content
            # unlikely to have two contact points with same content and meaningfully different types
            conts = [x['content'].strip() for x in rec['contact_point']]
            for i in merge['contact_point']:
                cont = i['content'].strip()
                if not cont in conts:
                    rec['contact_point'].append(i)
                    conts.append(cont)


    def merge_person(self, rec, merge, msource, skip):
        # born, died, carried_out, contact_point, residence
        if 'born' in merge and not 'born' in skip:
            self.merge_common(rec['born'], merge['born'], msource)

        if 'died' in merge and not 'died' in skip:
            self.merge_common(rec['died'], merge['died'], msource)
        self.merge_actor(rec, merge, skip, msource)           

    def merge_group(self, rec, merge, msource, skip):
        # formed_by, dissolved_by, carried_out, contact_point, residence
        if 'formed_by' in merge and not 'formed_by' in skip:
            self.merge_common(rec['formed_by'], merge['formed_by'], msource)

        if 'dissolved_by' in merge and not 'dissolved_by' in skip:
            self.merge_common(rec['dissolved_by'], merge['dissolved_by'], msource)

        self.merge_actor(rec, merge, msource, skip)

    def merge_humanmadeobject(self, rec, merge, msource, skip):
        # dimension, part_of, made_of, current_owner, current_custodian,
        # current_location, current_permanent_location, carries,
        # shows, produced_by, destroyed_by, removed_by
        # LUX: encountered_by

        # references
        refs = ['part_of', 'made_of', 'current_owner', 'current_custodian', 'current_location',
                'current_permanent_location', 'carries', 'shows']
        for rp in refs:
            if rp in merge and not rp in skip:
                # Assume that reconciliation has worked, so same related entities have same id.
                try:
                    ids = [x['id'] for x in rec[rp] if 'id' in x]
                except:
                    continue
                for i in merge[rp]:
                    if 'id' in i and not i['id'] in ids:
                        rec[rp].append(i)
                        ids.append(i['id'])                              
        # Events
        evts = ['produced_by', 'destroyed_by']
        part_skips = ['carried_out_by', 'took_place_at', 'influenced_by', 'caused_by']
        for ep in evts:
            if ep in merge and not ep in skip:
                self.merge_common(rec[ep], merge[ep], msource, part_skips)
        setevts = ['removed_by', 'encountered_by']
        # Can be multiple, so need to somehow determine identity :(
        # FIXME: for now assume only one
        for sep in setevts:
            if sep in merge and not sep in skip:
                self.merge_common(rec[sep][0], merge[sep][0], msource)

        # Dimensions only match if type, value and unit are identical
        if 'dimension' in merge and not 'dimension' in skip:
            for dm in merge['dimension']:
                found = False
                for dr in rec['dimension']:                      
                    if 'unit' in dr and 'unit' in dm:
                        try:
                            if dm['unit']['id'] == dr['unit']['id'] and dm['value'] == dr['value'] and \
                                dm['classified_as'][0]['id'] == dr['classified_as'][0]['id']:
                                # These are the same
                                found = True
                                break
                        except:
                            # probably broken id so don't merge anyway
                            found = True
                            break
                if not found:
                    rec['dimension'].append(dm)

    def merge_linguisticobject(self, rec, merge, msource,  skip):
        # language, dimension, part_of, content, format, digitally_carried_by,
        # carried_by, about, refers_to, created_by, used_for

        # references
        refs = ['part_of', 'language', 'carried_by', 'digitally_carried_by',
                'about', 'refers_to']
        for rp in refs:
            if rp in merge and not rp in skip:
                # Assume that reconciliation has worked, so same related entities have same id.
                ids = [x['id'] for x in rec[rp] if 'id' in x]
                for i in merge[rp]:
                    if 'id' in i and not i['id'] in ids:
                        rec[rp].append(i)
                        ids.append(i['id'])                

        # FIXME: what to do with these if different
        values = ['content', 'format']
        for vp in values:
            if vp in merge and not vp in skip and rec[vp].strip() != merge[vp].strip():
                print(f"Can't merge {rec[vp]} and {merge[vp]} in {rec['id']}")

        # Single Event - created_by
        part_skips = ['carried_out_by', 'took_place_at', 'influenced_by', 'caused_by']
        if 'created_by' in merge and not 'created_by' in skip:
            self.merge_common(rec['created_by'], merge['created_by'], msource, part_skips)

        # Multi Event - used_for
        # Process publishing event
        # FIXME: Other events this was used_for

        # Dimensions only match if type, value and unit are identical
        if 'dimension' in merge and not 'dimension' in skip:
            for dm in merge['dimension']:
                found = False
                for dr in rec['dimension']:
                    if dm['unit']['id'] == dr['unit']['id'] and dm['value'] == dr['value'] and \
                        dm['classified_as'][0]['id'] == dr['classified_as'][0]['id']:
                        # These are the same
                        found = True
                        break
                if not found:
                    dr.append(dm)

    def merge_set(self, rec, merge, msource, skip):
        # dimension, created_by, 
        # LUX: about, members_exemplified_by, used_for, subject_to 

        # references
        refs = ['about']
        for rp in refs:
            if rp in merge and not rp in skip:
                # Assume that reconciliation has worked, so same related entities have same id.
                ids = [x['id'] for x in rec[rp]]
                for i in merge[rp]:
                    if not i['id'] in ids:
                        rec[rp].append(i)
                        ids.append(i['id'])                       

        # FIXME: members_exemplified_by

        # Single Event - created_by
        if 'created_by' in merge and not 'created_by' in skip:
            self.merge_common(rec['created_by'], merge['created_by'], msource)

        # FIXME: Multi Event - used_for


        # Dimensions only match if type, value and unit are identical
        if 'dimension' in merge and not 'dimension' in skip:
            for dm in merge['dimension']:
                found = False
                for dr in rec['dimension']:
                    if dm['unit']['id'] == dr['unit']['id'] and dm['value'] == dr['value'] and \
                        dm['classified_as'][0]['id'] == dr['classified_as'][0]['id']:
                        # These are the same
                        found = True
                        break
                if not found:
                    dr.append(dm)

    def merge_visualitem(self, rec, merge, msource, skip):
        # dimension, part_of, digitally_shown_by, shown_by, about, represents,
        # represents_entity_of_type, created_by, used_for

        # references
        refs = ['about', 'part_of', 'digitally_shown_by', 'shown_by', 'represents', \
                'represents_entity_of_type']
        for rp in refs:
            if rp in merge and not rp in skip:
                # Assume that reconciliation has worked, so same related entities have same id.
                ids = [x.get('id', None) for x in rec[rp] if x]
                for i in merge[rp]:
                    if 'id' in i and i['id'] and not i['id'] in ids:
                        rec[rp].append(i)
                        ids.append(i['id'])                

        # Single Event - created_by
        if 'created_by' in merge and not 'created_by' in skip:
            self.merge_common(rec['created_by'], merge['created_by'], msource)

        # FIXME: Multi Event - used_for

        # Dimensions only match if type, value and unit are identical
        if 'dimension' in merge and not 'dimension' in skip:
            for dm in merge['dimension']:
                found = False
                for dr in rec['dimension']:
                    if dm['unit']['id'] == dr['unit']['id'] and dm['value'] == dr['value'] and \
                        dm['classified_as'][0]['id'] == dr['classified_as'][0]['id']:
                        # These are the same
                        found = True
                        break
                if not found:
                    dr.append(dm)

    def merge_digitalobject(self, rec, merge, msource, skip):
        # dimension, part_of, format, conforms_to, digitally_carries, digitally_shows,
        # digitally_available_via, access_point created_by, used_for

        # FIXME: ...

        # Dimensions only match if type, value and unit are identical
        if 'dimension' in merge and not 'dimension' in skip:
            for dm in merge['dimension']:
                found = False
                for dr in rec['dimension']:
                    if dm['unit']['id'] == dr['unit']['id'] and dm['value'] == dr['value'] and \
                        dm['classified_as'][0]['id'] == dr['classified_as'][0]['id']:
                        # These are the same
                        found = True
                        break
                if not found:
                    dr.append(dm)

    def merge_activity(self, rec, merge, msource, skip):
        # part_of, timespan, took_place_at, caused_by, influenced_by, carried_out_by,
        # used_specific_object, part

        if 'timespan' in merge and not 'timespan' in skip:
            # Can only have one timespan, so have to pick
            # Can't actually merge field by field, as the display label matches the data
            # and hard to manage the set of fields independently
            rts = rec['timespan']
            mts = merge['timespan']
            if type(rts) == list:
                rts = rts[0]
            if type(mts) == list:
                mts = mts[0]
            rb = rts.get('begin_of_the_begin', '')
            re = rts.get('end_of_the_end', '')
            mb = mts.get('begin_of_the_begin', '')
            me = mts.get('end_of_the_end', '')

            # Fix end of the end seconds :P
            try:
                if re and int(rb[:4]) == int(re[:4])-1 and re[5:10] == '01-01' and rb[5:10] == '01-01':
                    rts['end_of_the_end'] = f"{int(re[:4])-1}-12-31T23:59:59Z"
                    re = rts['end_of_the_end']
                if me and int(mb[:4]) == int(me[:4])-1 and me[5:10] == '01-01' and mb[5:10] == '01-01':
                    mts['end_of_the_end'] = f"{int(me[:4])-1}-12-31T23:59:59Z"
                    me = mts['end_of_the_end']
            except:
                print(rec)
                print(rb)
                print(re)

            # Throw out estimated
            done = False
            if 'classified_as' in rts:
                # FIXME: Use real identifier
                for c in rts['classified_as']:
                    if c['_label'].lower() in ['estimated', 'possibly']:
                        rec['timespan'] = mts
                        done = True
                        break
            if not done:
                # Use most specific dates         
                # FIXME:  Convert to AD dates and then recalculate the distance?

                if rb and re:
                    if rb[-1] == "Z":
                        rb = rb[:-1]
                    if rb[0] == '-':
                        rb = rb[1:]
                        rb_bc = True
                    if re[-1] == "Z":
                        re = re[:-1]
                    if re[0] == '-':
                        re = re[1:]
                        re_bc = True                    
                    try:
                        rbt = datetime.strptime(rb, "%Y-%m-%dT%H:%M:%S")
                        if re[-1] == "Z":
                            re = re[:-1]
                        ret = datetime.strptime(re, "%Y-%m-%dT%H:%M:%S")                
                        rdelta = ret - rbt
                    except:
                        rdelta = timedelta(100000)
                else:
                    print(f"Record's timespan: {rts}")
                    rdelta = timedelta(100000)

                if mb and me:
                    if mb[-1] == "Z":
                        mb = mb[:-1]
                    if me[-1] == "Z":
                        me = me[:-1]
                    try:
                        mbt = datetime.strptime(mb, "%Y-%m-%dT%H:%M:%S")
                        met = datetime.strptime(me, "%Y-%m-%dT%H:%M:%S")
                        mdelta = met - mbt
                    except:
                        mdelta = None
                else:
                    # print(f"Merge's timespan: {mts}")
                    mdelta = None

                if mdelta and mdelta < rdelta:
                    rec['timespan'] = mts

        for rp in ['took_place_at', 'caused_by', 'influenced_by', 'carried_out_by',
                    'used_specific_object']:
            if rp in merge and not rp in skip:
                # Assume that reconciliation has worked, so same related entities have same id.
                if rp in rec:
                    ids = [x['id'] for x in rec[rp] if 'id' in x]
                else:
                    ids = []
                if 'part' in rec:
                    for p in rec['part']:
                        if rp in p:
                            for x in p[rp]:
                                if 'id' in x:
                                    ids.append(x['id'])
                for i in merge[rp]:
                    # FIXME: if haven't reconciled string places, they won't have ids :(
                    if 'id' in i and not i['id'] in ids:
                        if not rp in rec:
                            rec[rp] = []
                        rec[rp].append(i)
                        ids.append(i['id'])                      

        # further process Provenance and Exhibition top level for embedded parts
        if '@context' in rec and 'classified_as' in rec and 'part' in merge and not 'part' in skip:
            cids = [x['id'] for x in rec['classified_as']]
            if 'http://vocab.getty.edu/aat/300055863' in cids:
                self.merge_provenance_parts(rec, merge, msource, skip)            
            elif 'http://vocab.getty.edu/aat/300054766' in cids:
                self.merge_exhibition_parts(rec, merge, msource, skip)
            else:
                # FIXME: WTF is this record?
                print(f"Unknown activity record type with parts: {rec['id']}")

    def merge_provenance_parts(self, rec, merge, msource, skip):
        # FIXME: do something with parts
        pass       

    def merge_exhibition_parts(self, rec, merge, msource, skip):
        # FIXME: do something with parts
        pass

    def merge_common(self, rec, merge, msource="", myskip=None):

        if 'id' in merge:
            del merge['id']

        # Check if type is the same, if not, log an issue
        if rec['type'] != merge['type']:
            # FIXME: make this log somewhere sensible
            # Prefer Language (etc) over Type
            if rec['type'] in ['Language', 'Material', 'MeasurementUnit', 'Currency'] and merge['type'] == 'Type':
                pass
            elif merge['type'] in ['Language', 'Material', 'MeasurementUnit', 'Currency'] and rec['type'] == 'Type':
                rec['type'] = merge['type']
            else:
                # print(f"Trying to merge records with different classes: {rec.get('id', '')}={rec['type']} / {merge.get('id', '')}={merge['type']}")
                # Just don't do it
                return []
        del merge['type']

        # Do common fields here

        skip = []
        if myskip is None:
            myskip = []

        # Copy if doesn't exist, and aren't in myskip
        for k in merge.keys():
            if not k in rec and not k in myskip:
                rec[k] = merge[k]
                skip.append(k)

        primaryName = self.globals['primaryName']
        alternateName = self.globals['alternateName']

        if 'identified_by' in merge and not 'identified_by' in skip:
            # FIXME: check part: merge may have the same name split up into parts
            nm_conts = {x['content'].strip().lower():x for x in rec['identified_by'] if x['type'] == 'Name' and 'content' in x and x['content']}
            id_conts = {str(x['content']).strip().lower():x for x in rec['identified_by'] if x['type'] == 'Identifier' and 'content' in x and x['content']}

            has_primary = False
            for nm in rec['identified_by']:
                for cx in nm.get('classified_as', []):
                    if 'id' in cx and cx['id'] == primaryName:
                        has_primary = True
                        break
                if has_primary:
                    break

            is_internal = msource in self.internal_sources

            for i in merge['identified_by']:                
                try:
                    cont = i['content'].strip()
                except:
                    # no content, no need to add
                    continue
                which = nm_conts if i['type'] == 'Name' else id_conts

                if not cont.lower() in which:
                    # if we have a primary name already, then strip primary name from others
                    remove = []
                    for cx in i.get("classified_as", []):
                        if 'id' in cx and cx['id'] == primaryName:
                            remove.append(cx)
                    if has_primary:
                        # Don't trash primary from any internal record
                        # final mapper will select one
                        if not is_internal:
                            for r in remove:
                                i['classified_as'].remove(r)
                    else:
                        has_primary = True
                    rec['identified_by'].append(i)
                    which[cont.lower()] = i
                elif i['type'] == 'Name':
                    # test if it's primary in b and set in a
                    main = nm_conts[cont.lower()]
                    if 'classified_as' in i:
                        icxns = [x.get('id', None) for x in i['classified_as']]
                        mcxns = [x.get('id', None) for x in main.get('classified_as', [])]

                        for ic in icxns:
                            if ic == primaryName:
                                if not has_primary and not alternateName in mcxns:
                                    if not 'classified_as' in main:
                                        main['classified_as'] = []
                                    main['classified_as'].append({'id': primaryName, 'type':'Type', '_label':'Merge: Primary Name'})
                                    has_primary = True
                                else:
                                    # do nothing
                                    pass
                            elif ic == alternateName:
                                if not primaryName in mcxns and not alternateName in mcxns:
                                    if not 'classified_as' in main:
                                        main['classified_as'] = []
                                    main['classified_as'].append({'id': alternateName, 'type':'Type', '_label':'Merge: Alternate Name'})
                            elif not ic in mcxns:
                                if not 'classified_as' in main:
                                    main['classified_as'] = []
                                main['classified_as'].append({'id': ic, 'type':'Type'})
                            else:
                                # already have it or otherwise don't want it 
                                pass

                    if 'language' in i:
                        if not 'language' in main:
                            main['language'] = i['language']
                        else:
                            mlangs = [x.get('id', None) for x in main['language']]                            
                            mlangs.append(None) # so no language in i won't match
                            for l in i['language']:
                                if not l.get('id', None) in mlangs:
                                    main['language'].append(l)

        if 'referred_to_by' in merge and not 'referred_to_by' in skip:
            try:
                conts = [x.get('content', '').strip() for x in rec['referred_to_by']]
            except:
                # if this breaks, everything else will too
                print(f"Broken ref_to_by in {rec}")
                conts = []
            if conts:
                ids = [x.get('id', '') for x in rec['referred_to_by']]
                for i in merge['referred_to_by']:
                    if 'content' in i:
                        if type(i['content']) == list:
                            if not i['content']:
                                i['content'] = ""
                            else:
                                i['content'] = i['content'][0]
                        cont = i['content'].strip()
                        if not cont in conts:
                            rec['referred_to_by'].append(i)
                            conts.append(cont)
                    elif 'id' in i:
                        if not i['id'] in ids:
                            rec['referred_to_by'].append(i)
                            ids.append(i['id'])         

        if 'subject_of' in merge and not 'subject_of' in skip:
            # filter by access_point
            rec_aps = []
            for s in rec['subject_of']:
                if 'digitally_carried_by' in s:
                    for do in s['digitally_carried_by']:
                        if 'access_point' in do and 'id' in do['access_point'][0]:
                            rec_aps.append(do['access_point'][0]['id'])
            for s2 in merge['subject_of']:
                if 'digitally_carried_by' in s2:
                    for do in s2['digitally_carried_by']:
                        if 'access_point' in do and 'id' in do['access_point'][0] and \
                            not do['access_point'][0]['id'] in rec_aps:
                            rec['subject_of'].append(s2)  

        for rp in ['classified_as', 'equivalent', 'member_of']:
            if rp in merge and not rp in skip:
                ids = [x['id'] for x in rec[rp] if 'id' in x]
                for i in merge[rp]:
                    if 'id' in i and not i['id'] in ids:
                        rec[rp].append(i)
                        ids.append(i['id'])

        if 'representation' in merge and not 'representation' in skip:
            # Simple apid equality merge
            # and let final mapper deal with filtering
            curr = []
            for ap in rec['representation']:
                try:
                    curr.append(ap['digitally_shown_by'][0]['access_point'][0]['id'])
                except:
                    pass
            for rep in merge['representation']:
                try:
                    rap = rep['digitally_shown_by'][0]['access_point'][0]['id']
                except:
                    print(f" -- no dsb or access_point in {rep}")
                    continue
                if rap and not rap in curr:
                    rec['representation'].append(rep)

        # Now do class dependent 
        # Note that this could be during recursion 
        #  (person:  common -> person; birth: common -> activity)
        t = rec['type'].lower()
        if hasattr(self, f"merge_{t}"):
            fn = getattr(self, f"merge_{t}")
            fn(rec, merge, msource, skip)


    def should_merge(self, base, to_merge):

        base_src = base['source']
        merge_src = to_merge['source']
        if base_src in self.configs.internal:
            base_config = self.configs.internal[base_src]
        elif base_src in self.configs.external:
            base_config = self.configs.external[base_src]
        else:
            print(f"Unknown base source in merge: {base_src}")
            return True

        if merge_src in self.configs.internal:
            merge_config = self.configs.internal[merge_src]
        elif merge_src in self.configs.external:
            merge_config = self.configs.external[merge_src]            
        else:
            print(f"Unknown merge source in merge: {merge_src}")
            return True

        if 'mapper' in base_config:
            bmap = base_config['mapper']
            bokay = bmap.should_merge_into(base, to_merge)
        else:
            # No mapper?? Assume okay
            bokay = True

        if 'mapper' in merge_config:
            mmap = merge_config['mapper']
            mokay = mmap.should_merge_from(base, to_merge)
        else:
            mokay = True

        return bokay and mokay



    ### API ###

    def merge(self, base, to_merge):
        # Merge data from `to_merge` into `base`

        # rec, merge are wrapped with meta-metadata, to enable source dependent processing
        # We need this for tracking sources, so fail if not present

        if 'data' in base:
            rec = base['data']
        else:
            print(base)
            raise ValueError("Passed in a non-record as base")

        if 'data' in to_merge and 'source' in to_merge:
            merge = to_merge['data']
            msource = to_merge['source']
        else:
            raise ValueError("Passed in a non-record as merge")

        if rec['id'] != merge['id']:
            # Test if we're merging records after merged identifiers
            if 'equivalent' in merge:
                try:
                    eqid = merge['equivalent'][0]['id']
                except:
                    print(f"No id in equivalent[0]: {merge['equivalent']}")
                    return rec
                if 'equivalent' in rec:
                    reqs = [x['id'] for x in rec['equivalent']]
                else:
                    reqs = []
                if eqid in reqs:
                    # Yup, we're good
                    merge['id'] = rec['id']
                elif eqid.endswith('/') and eqid[:-1] in reqs:
                    # Still good
                    merge['id'] = rec['id']
                else:
                    print("Failed to detect identifier merge:")
                    print(f"Main record: {rec['id']} = {reqs}")
                    print(f"Merging: {merge['id']} = {eqid}")
                    #print(f"Merge eq in idmap: {self.idmap[eqid]}")
                    # Something has gone HORRIBLY wrong
                    # raise ValueError()
                    return rec

        if self.should_merge(base, to_merge):
            # delete context
            del merge['@context']
            self.merge_common(rec, merge, msource)
            base['sources'].append(to_merge['source'])
        else:
            try:
                print(f"Did not merge {to_merge['identifier']} into {base['yuid']}")
            except:
                print(f"Skipped merging {to_merge} into {base}")
        return base
