from lux_pipeline.process.base.harvester import ASProtocol


class GettyProtocol(ASProtocol):
    """Wrap the regular ActivityStreams Protocol to rewrite the URIs"""

    def process_items(self, items, refsonly=False):
        # self.namespace = f"http://data.getty.edu/vocab/{self.prefix}/"
        filtered_items = []
        for item in items:
            try:
                what = item["object"]["id"]
            except:
                continue
            # self.prefix is the name of the vocabulary
            if f"/{self.prefix}/" in what:
                # https://data.getty.edu/vocab/aat/300404670
                # --> http://vocab.getty.edu/aat/300404670
                ident = what.rsplit("/", 1)[-1]
                item["object"]["id"] = f"{self.namespace}{ident}"
                filtered_items.append(item)
        self.manager.increment_progress_bar(len(items) - len(filtered_items))
        if filtered_items:
            for rec in super().process_items(filtered_items, refsonly):
                yield rec
        else:
            return []
