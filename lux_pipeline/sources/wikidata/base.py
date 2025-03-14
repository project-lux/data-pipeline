
# Process configs needed for multiple classes

class WdConfigManager(object):

    def __init__(self, config): 
        self.config = config
        self.configs = config['all_configs']
        self.property_to_source = {}

        srcs = [self.configs.internal, self.configs.external, self.configs.results]
        for stype in srcs:
            for source in stype.values():
                if 'wikidata_property' in source and source['wikidata_property']:
                    wdp = source['wikidata_property']
                    if type(wdp) != list:
                        wdp = [wdp]
                    for p in wdp:
                        try:
                            self.property_to_source[p].append(source)
                        except:
                            self.property_to_source[p] = [source]

        # Make sure to update the inverse in reconciler
        # if you add something to pref_map
        self.pref_map = {
            'P244': 'lc',
            'P2163': 'fast',
            'P214': 'viaf',
            'P1014': 'aat',
            'P245': 'ulan',
            'P1667': 'tgn',
            'P8902': 'aspace',
            'P1566': 'geonames',
            'P6766': 'wof',
            'P846': 'gbif',
            'P830': 'eol',
            'P6944': 'bionomia',
            'P243': 'oclcnum',
            'P305': 'lang',
            'P4801': 'lcvoc',
            'P1149': 'lcc',
            'P269': 'idref',
            'P227': 'gnd',
            'P213': 'isni',
            'P11858': 'nsf',
            'P3500': 'ringgold',
            'P6782': 'ror', # https://ror.org/00qb4ds53
            'P3430': 'snac', # https://snaccooperative.org/ark:/99166/{ident}
            'P496': 'orcid', # https://orcid.org/<ident>
            'P8516': 'lcpm', # https://id.loc.gov/authorities/performanceMediums/{ident}
            'P3763': 'mimo', # http://www.mimo-db.eu/InstrumentsKeywords/{ident}
            'P402': 'osm', # Open Street Map relation id
            'P349': 'ndl', # Japan
            'P5587': 'snl' # Sweden
        }
        self.different_prop = "P1889"
        self.instance_of_prop = "P31"
        self.disambiguations = ["Q4167410"]

    def process_equivs(self, data):
        rec = data['data']
        recid = rec['id']
        sames = []
        diffs = []

        for prop, pref in self.pref_map.items():
            if prop in rec:
                val = rec[prop]
                if type(val) == str:
                    val = [val]
                elif type(val) == list:
                    pass
                else:
                    print(f"unknown identifier value type: {val}")
                    continue
                for i in val:
                    if type(i) != str:
                        print(f"Unknown identifier item value type: {i}")
                    else:
                        sames.append((f"{pref}:{i}", recid))

        if self.different_prop in rec:
            val = rec[self.different_prop]
            if type(val) == str:
                val = [val]

            is_disambig = False
            if self.instance_of_prop in rec:
                types = rec[self.instance_of_prop]
                if type(types) == str:
                    types = [types] 
                for d in self.disambiguations:
                    if d in types:
                        is_disambig = True
                        break

            if is_disambig:
                for x in val:
                    for y in val:
                        if x != y:
                            diffs.append((x,y))
            else:
                # Regular record
                for i in val:
                    if type(i) == str:
                        diffs.append((i, recid))

        return (sames, diffs)