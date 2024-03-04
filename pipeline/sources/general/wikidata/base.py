
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
