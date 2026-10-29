import logging
from lux_pipeline.process.base.harvester import ASHarvester, ASProtocol
import ujson as json
import sys

logger = logging.getLogger("lux_pipeline")


class GettyProtocol(ASProtocol):
    """Wrap the regular ActivityStreams protocol to rewrite the URIs.

    The Getty vocabularies share one stream; keep only this vocabulary's
    items and rewrite ``https://data.getty.edu/vocab/aat/300404670`` to
    ``<namespace>300404670`` (e.g. ``http://vocab.getty.edu/aat/300404670``).
    """

    def process_items(self, items, refsonly=False):
        filtered_items = []
        for item in items:
            try:
                what = item["object"]["id"]
            except Exception:
                continue
            # self.prefix is the name of the vocabulary
            if f"/{self.prefix}/" in what:
                ident = what.rsplit("/", 1)[-1]
                item["object"]["id"] = f"{self.namespace}{ident}"
                filtered_items.append(item)
        skipped = len(items) - len(filtered_items)
        if skipped:
            self.progress(advance=skipped)
        if filtered_items:
            yield from super().process_items(filtered_items, refsonly)


class GettyHarvester(ASHarvester):
    protocol_class = GettyProtocol


class GettyHarvester2(ASHarvester):

    def __init__(self, config):
        ASHarvester.__init__(self, config)
        self.uris = []

    def process_items(self, items): 
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
        with open(fn, 'w') as fh:
            fh.write(jstr)
