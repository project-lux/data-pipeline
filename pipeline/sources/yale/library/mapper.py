from pipeline.process.base.mapper import Mapper
from pipeline.process.utils.mapper_utils import validate_timespans
from pipeline.process.utils.mapper_utils import make_datetime
import os
import ujson as json
import csv

multi_props = ['part_of', 'identified_by', 'classified_as', 'equivalent', 'member_of', 'subject_of', \
    'referred_to_by', 'influenced_by', 'about', 'carries', 'shows', 'attributed_by', 'carried_out_by', \
    'took_place_at']

single_props = ['timespan', 'produced_by', 'created_by', 'content', 'begin_of_the_begin', 'end_of_the_end', 'value']


class YulMapper(Mapper):
    def __init__(self, config):
        Mapper.__init__(self, config)
        lccns = {}
        cfgs = config['all_configs']
        data_dir = cfgs.data_dir
        fn = os.path.join(data_dir, 'stupid_table.json')
        self.object_work_mismatch = {}
        if os.path.exists(fn):
            fh = open(fn)
            self.object_work_mismatch = json.load(fh)
            fh.close()
        # stubid --> [real_id, type]

        ycbaexhs = {}
        ycbaobjs = {}
        fn1 = os.path.join(data_dir,'ils-exhs.csv')
        fn2 = os.path.join(data_dir,'ils-objs.csv')
        if os.path.exists(fn1):
            with open(fn1,'r') as file:
                reader = csv.reader(file)
                exhslist = list(reader)
                for e in exhslist:
                    ycbaexhs[e[0]] = e[1:]
        if os.path.exists(fn2):
            with open(fn2,'r') as file:
                reader = csv.reader(file)
                objslist = list(reader)
                for o in objslist:
                    ycbaobjs[o[0]] = o[1:]
        self.ycbaobjs = ycbaobjs
        self.ycbaexhs = ycbaexhs

        fn = os.path.join(data_dir, 'lib_okay_parts.json')
        fh = open(fn)
        okay = json.load(fh)
        fh.close()
        self.okay_place_parts = okay

        self.wiki_recon = {}
        fn = os.path.join(data_dir, 'yul-wiki-recon.csv')
        if os.path.exists(fn):
            with open(fn, 'r') as fh:
                reader = csv.reader(fh)
                for row in reader:
                    self.wiki_recon[row[0]] = row[1]


    def walk_multi(self, node, top=False):
        for (k,v) in node.items():
            if k in multi_props and not type(v) == list:
                node[k] = [v]
                v = [v]
            if k in single_props and type(v) == list:
                node[k] = v[0]
            if not top and 'id' in node and node['id'] in self.object_work_mismatch:
                node['id'] = self.object_work_mismatch[node['id']][0]
                node['type'] = self.object_work_mismatch[node['id']][1]


            if type(v) == list:
                for vi in v:
                    if type(vi) == dict:
                        self.walk_multi(vi)
                    else:
                        print(f"found non dict in a list :( {node}")
            elif type(v) == dict:
                self.walk_multi(v)



    def transform(self, rec, rectype, reference=False):

        data = rec['data'] 

        if data['id'] in self.object_work_mismatch:
            return None

        # don't process part_of                
        if data['type'] == 'LinguisticObject' and 'part_of' in data:
            # if only id, type, label, part_of, identified_by, then kill it 
            # it's a left over table of contents record
            min_keys = {'@context', 'id', 'type', '_label', 'part_of', 'identified_by'}
            if set(data.keys()) == min_keys:
                print(f"Skipping part_of record {rec['identifier']}")
                return None


        #add abouts for ycba exhs & objs
        if data['type'] == 'LinguisticObject':
            aboutblock = data.get('about',[])
            #get yul ID #
            for ident in data['identified_by']:
                if ident['content'].startswith('ils:yul:'):
                    ilsnum = ident['content'].split(':')[-1]
                    break
            try:
                objslist = self.ycbaobjs[ilsnum]
                for objs in objslist:
                    if objs != "":
                        objsblock = {"id":objs,"type":"HumanMadeObject"}
                        aboutblock.append(objsblock)
            except:
                pass 
            try:
                exhslist = self.ycbaexhs[ilsnum]
                for exhs in exhslist:
                    if exhs != "":
                        exhsblock = {"id":exhs,"type":"Activity"}
                        aboutblock.append(exhsblock)
            except:
                pass

            if aboutblock != []:
                data['about'] = aboutblock


        if data['id'] in self.wiki_recon:
            equivs = data.get('equivalent', [])
            equivs.append({"id":self.wiki_recon[data['id']], "type": data['type'], '_label': data.get('_label', 'wikidata equivalent')})
            data['equivalent'] = equivs

        # modify in place
        ### 2021-12-01 
        self.walk_multi(data, top=True)
        validate_timespans(data)

        # 2022-08-1 -- Digitally_shown_by is still wrong. id needs to move to access_point
        # Move representation and digitally_shown_by to subject_of
        # EG https://linked-art.library.yale.edu/node/07e71913-f2be-479c-a388-65025173b4fb 
        #    https://linked-art.library.yale.edu/node/14a3551f-6e93-4501-9b18-1b107f0274aa

        # rewrite exemplary_member_of to just member_of
        # We wouldn't do anything differently between the two predicates
        if 'exemplary_member_of' in data:
            data['member_of'] = data['exemplary_member_of']
            del data['exemplary_member_of']

        for key in ['representation', 'digitally_shown_by', 'digitally_carried_by']:
            if key in data:
                del_reps = []
                aps = []                    
                for r in data[key]:
                    if 'id' in r and not "linked-art" in r['id']:
                        # Access point
                        ap = r['id']
                        if 'identified_by' in r:
                            names = r['identified_by']
                        else:
                            names = []
                        aps.append((ap, names))
                        del_reps.append(r)
                    elif 'type' in r and r['type'] == 'VisualItem' and 'digitally_shown_by' in r:
                        kill = False
                        for dsb in r['digitally_shown_by']:
                            if 'id' in dsb:
                                ap = dsb['id']
                                if 'identified_by' in dsb:
                                    names = dsb['identified_by']
                                else:
                                    names = []
                                aps.append((ap, names))
                                kill = True
                        if kill:
                            del_reps.append(r)

                for d in del_reps:
                    try:
                        data[key].remove(d)
                    except:
                        print(f"Failed to remove {d} from {data[key]}")
                if not data[key]:
                    del data[key]
                if aps:
                    # Move to subject_of
                    if not 'subject_of' in data:
                        data['subject_of'] = []
                    for (ap, names) in aps:
                        a = {'type':"DigitalObject", 'access_point': [{'id': ap, 'type':'DigitalObject'}]}
                        if names:
                            a['identified_by'] = names
                        data['subject_of'].append({'type': 'LinguisticObject', 
                            '_label': 'Representation/Reference', 'digitally_carried_by': [a]})

        # Trash all part_ofs for SBX testing June 5 2024
        if data['type'] == 'Place' and 'part_of' in data:
            del data['part_of']


        # Swap MarcGT to AAT equivalents
        if 'classified_as' in data:
            class_as = []
            for cxns in data['classified_as']:
                if cxns['id'] == "http://id.loc.gov/vocabulary/marcgt/rea":
                    cxns['id'] = "http://vocab.getty.edu/aat/300265419"
                elif cxns['id'] == "http://id.loc.gov/vocabulary/marcgt/pic":
                    cxns['id'] = "http://vocab.getty.edu/aat/300264388"               

        # Add collection item flag
        # FIXME: This doesn't work for archives

        item = False
        if data['type'] in ['HumanMadeObject', 'DigitalObject']:
            item = True
        elif "identified_by" in data:
            ids = [x for x in data['identified_by'] if x['type'] == 'Identifier']
            for i in ids:
                if 'classified_as' in i:
                    for c in i['classified_as']:
                        if 'id' in c and c['id'] == "http://vocab.getty.edu/aat/300435704" and \
                            'content' in i and i['content'].startswith('ils:yul:') and \
                            not i['content'].startswith('ils:yul:mfhd:'):
                            # we're an item
                            item = True
                            break
                        elif not 'id' in c or not 'content' in i:
                            # FIXME: Fix this based on _label?
                            pass
        if item:
            if 'classified_as' in data:
                classes = data['classified_as']
            else:
                classes = []
            classes.append({"id": "http://vocab.getty.edu/aat/300404024", "type":"Type", "_label": "Collection Item"})
            data['classified_as'] = classes


        if 'defined_by' in data and data['defined_by'] == "":
            del data['defined_by']

        if 'identified_by' in data:
            for ident in data['identified_by']:
                if 'attributed_by' in ident:
                    ident['assigned_by'] = ident['attributed_by']
                    del ident['attributed_by']
                if 'classified_as' in ident:
                    for c in ident['classified_as']:
                        cxnid = c.get('id','')
                        if cxnid and cxnid.startswith('https://vocab.getty.edu'):
                            c['id'] = cxnid.replace('https://','http://')

        if data['type'] == 'Period':
            #Add classification of AAT Period
            if 'classified_as' in data:
                classes = data['classified_as']
                classes.append({"id": "http://vocab.getty.edu/aat/300081446", "type":"Type", "_label": "Period"})
            else:
                data['classified_as'] = [{"id": "http://vocab.getty.edu/aat/300081446", "type":"Type", "_label": "Period"}]

            #try to parse dates for timespan
            if not "timespan" in data:
                if "identified_by" in data:
                    for i in data["identified_by"]:
                        if "classified_as" in i:
                            for c in i['classified_as']:
                                if "id" in c:
                                    if c['id'] == "http://vocab.getty.edu/aat/300404670":
                                        cont = i.get("content","")
                                        if cont and "," in cont:
                                            dates = cont.rsplit(",",1)[-1].strip()
                                            try:
                                                b,e = make_datetime(dates)
                                            except:
                                                b = e = None
                                            template = {"type":"TimeSpan","begin_of_the_begin":"","end_of_the_end":"","identified_by":[{"type":"Name","classified_as":[{"id":"http://vocab.getty.edu/aat/300404669","type":"Type","_label":"Display Title"}],"content":dates}]}
                                            if b and e:
                                                template["begin_of_the_begin"] = b
                                                template["end_of_the_end"] = e
                                            elif b:
                                                template["begin_of_the_begin"] = b
                                            elif e:
                                                template["end_of_the_end"] = e
                                            if b or e:
                                                data['timespan'] = template

                            
        return rec
