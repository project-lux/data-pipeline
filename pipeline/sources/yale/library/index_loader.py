import csv
from pipeline.process.base.index_loader import LmdbIndexLoader, TabLmdb


class YulIndexLoader(LmdbIndexLoader):
    def get_storage(self):
        mapExp = self.config.get("mapSizeExponent", 30)

        headings_path = self.config.get("headingsPath", None)
        if headings_path:
            index = TabLmdb.open(headings_path, "c", map_size=2**mapExp, readahead=False, writemap=True)

            if "__init__" not in index:
                index["__init__"] = "init"
        else:
            index = None

        return index

    def load_index(self):
        headings_index = self.get_storage()[0]

        return headings_index

    def clear(self):
        headings_index = self.load_index()
        headings_index.clear()

    def set(self, idx, key, vals):
        if type(vals) != list:
            raise ValueError(f"Called set with string {key}:{vals}, did you mean add()?")
        idx[key] = vals

    def add(self, idx, key, val):
        # Add val to the list or create
        try:
            vals = idx[key]
            if type(vals) == str:
                vals = [vals]
        except:
            vals = []
        vals.append(val)
        idx[key] = vals

    def update(self, filename):
        headings_index = self.load_index()

        if headings_index is None:
            print(f"{self.name} has no indexes configured")
            return None

        updates = {}
        # new additions to the csv
        with open(filename) as csvfh:
            rdr = csv.reader(csvfh, delimiter=",")
            for row in rdr:
                if len(row) < 2:
                    continue

                key = row[0]
                values = row[1:]

                updates.setdefault(key, []).extend(values)
        
        try:
            existing = headings_index[key]
            if isinstance(existing, str):
                existing = [existing]
        except KeyError:
            existing = []

        merged = list(set(existing + new_vals))
        headings_index[key] = merged
