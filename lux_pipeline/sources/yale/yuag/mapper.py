from lux_pipeline.process.base.mapper import Mapper
from lux_pipeline.process.utils.mapper_utils import validate_timespans
import ujson as json

class YuagMapper(Mapper):

    def transform(self, rec, rectype=None, reference=False):
        rec = Mapper.transform(self, rec, rectype)

        primaryNameJson = {"id": "http://vocab.getty.edu/aat/300404670",
            "type": "Type",
            "_label": "Primary Name"
            }

        data = rec['data']
        # modify in place

        # 2022-08-19
        # Agents are all sorts of messed up
        # formed_by --> obj not list
        if 'formed_by' in data and type(data['formed_by']) == list:
            data['formed_by'] = data['formed_by'][0]
        if 'dissolved_by' in data and type(data['dissolved_by']) == list:
            data['dissolved_by'] = data['dissolved_by'][0]
        # Person/Group confusion
        if data['type'] == 'Group' and ('born' in data or 'died' in data):
            data['type'] = 'Person'
            if 'equivalent' in data:
                for eq in data['equivalent']:
                    eq['type'] = 'Person'

        if "_used_for" in data:
            data['used_for'] = data['_used_for']
            del data['_used_for']

        if 'referred_to_by' in data:
            to_kill = []
            for rtb in data['referred_to_by']:
                if not 'content' in rtb:
                    to_kill.append(rtb)
            for k in to_kill:
                data['referred_to_by'].remove(k)

        # identified_as :(
        if 'identified_as' in data:
            data['identified_by'] = data['identified_as']
            del data['identified_as']

        # Missing ID in classifications
        # https://media.art.yale.edu/content/lux/agt/1562.json 
        if 'classified_as' in data:
            to_kill = []
            for c in data['classified_as']:
                if not 'id' in c:
                    to_kill.append(c)
            for k in to_kill:
                data['classified_as'].remove(k)

        # At least exhibitions have integer Identifier content
        if 'identified_by' in data:
            to_kill = []
            for i in data['identified_by']:
                if 'id' in i:
                    del i['id']
                if i['type'] == 'Primary Name':
                    i['type'] = 'Name'
                    i['classified_as'] = [primaryNameJson]
                if 'content' in i:
                    if type(i['content']) != str:
                        # probably a real number
                        i['content'] = str(i['content'])
                else:
                    # uhh, trash it
                    to_kill.append(i)
            if to_kill:
                for k in to_kill:
                    data['identified_by'].remove(k)

        # 2022-08-17
        # https://media.art.yale.edu/content/lux/exb/4603.json
        if data['type'] == 'Activity':
            if not 'timespan' in data and 'part_of' in data and 'timespan' in data['part_of'][0]:
                data['timespan'] = data['part_of'][0]['timespan']
                del data['part_of']
            if 'took_place_at' in data:
                cobs = []
                tpas = []
                for tpa in data['took_place_at']:
                    if 'id' in tpa and '/agt/' in tpa['id']:
                        # should be carried out by
                        tpa['type'] = 'Group'
                        cobs.append(tpa)
                    else:
                        tpa['type'] = 'Place'
                        tpas.append(tpa)
                if cobs:
                    data['carried_out_by'] = cobs
                if tpas:
                    data['took_place_at'] = tpas
                else:
                    del data['took_place_at']

        # 2022-08-15
        # https://media.art.yale.edu/content/lux/obj/307686.json
        # Dimension units shouldn't be in lists
        # Axis - not useful from nomisma
        # Instead:  http://nomisma.org/id/axis to 
        #           https://www.wikidata.org/entity/Q5141550 
        # https://media.art.yale.edu/content/lux/obj/170979.json 
        # https://media.art.yale.edu/content/lux/obj/31619.json 

        if False:
            if 'dimension' in data:
                for d in data['dimension']:
                    if 'unit' in d and type(d['unit']) == list:
                        d['unit'] = d['unit'][0]
                        if 'id' in d['unit']:
                            if d['unit']['id'] == "http://nomisma.org/id/axis":
                                d['unit']['id'] = "https://www.wikidata.org/entity/Q5134807"
                            elif d['unit']['id'] == "http://vocab.getty.edu/page/300379240":
                                d['unit']['id'] == "http://vocab.getty.edu/aat/300379240"                            

                    if 'classified_as' in d:
                        for cxn in d['classified_as']:
                            if 'id' in cxn:
                                if cxn['id'] == "http://nomisma.org/id/axis":
                                    cxn['id'] = "https://www.wikidata.org/entity/Q5141550"
                                elif cxn['id'] == "http://vocab.getty.edu/page/aat/300379240":
                                    cxn['id'] = "http://vocab.getty.edu/aat/300443981"
            if 'referred_to_by' in data:
                for rtb in data['referred_to_by']:
                    if 'classified_as' in rtb:
                        for cxn in rtb['classified_as']:
                            if 'id' in cxn and cxn['id'] == "http://vocab.getty.edu/aat/300-rights-stmt":
                                cxn['id'] = "http://vocab.getty.edu/aat/300435434"


        validate_timespans(data)

        # 2022-01-14: Add item classified_as to make finding items easier
        # Rely on things in collection having accession numbers
        item = False
        if "identified_by" in data:
            ids = [x for x in data['identified_by'] if x['type'] == 'Identifier']
            for i in ids:
                if 'classified_as' in i:
                    for c in i['classified_as']:
                        if c['id'] == "http://vocab.getty.edu/aat/300312355":
                            # we're an item
                            item = True
                            break
        if item:
            if 'classified_as' in data:
                classes = data['classified_as']
            else:
                classes = []
            classes.append({"id": "http://vocab.getty.edu/aat/300404024", "type":"Type", "_label": "Collection Item"})
            data['classified_as'] = classes


        return rec

