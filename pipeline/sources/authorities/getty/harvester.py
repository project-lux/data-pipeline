from pipeline.process.base.harvester import ASHarvester
import json
import sys

class GettyHarvester(ASHarvester):
    
    def process_items(self, items): 
        self.namespace = f"http://data.getty.edu/vocab/{self.prefix}/"
        filtered_items = []
        for item in items:
            try:
                what = item['object']['id']  
            except:
                continue
            # self.prefix is the name of the vocabulary
            if f"/{self.prefix}/" in what:
                filtered_items.append(item)
        if filtered_items:
            Harvester.process_items(self, filtered_items)
                                                     
class GettyHarvester2(ASHarvester):

    def __init__(self, config):
        ASHarvester.__init__(self, config)
        self.uris = []

    def process_items(self, items): 
        sys.stdout.write('x');sys.stdout.flush()
        for item in items:
            try:
                what = item['object']['id']  
            except:
                continue
            # self.prefix is the name of the vocabulary
            if f"/{self.prefix}/" in what:
                self.uris.append(what.replace(f'https://data.getty.edu/vocab/{self.prefix}/', ''))

    def crawl(self):
        try:
            Harvester.crawl(self)
        except:
            pass
        jstr = json.dumps(self.uris)
        fn = os.path.join(self.config['all_configs'].data_dir, f'getty_{self.prefix}_uris.json')
        fh = open(fn, 'w')
        fh.write(jstr)
        fh.close()
