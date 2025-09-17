import os
import csv
import ujson as json
from pipeline.process.base.index_loader import LmdbIndexLoader, TabLmdb
from pipeline.storage.idmap.lmdb import JsonLmdb


class LlmNameIndexLoader(LmdbIndexLoader):
    def get_storage(self):
        mapExp = self.config.get("mapSizeExponent", 30)
        path = self.config.get("llmNameDbPath", os.path.join(self.config.get("indexes_dir"), "llmPersonNames.lmdb"))
        return JsonLmdb.open(path, "c", map_size=2**mapExp, readahead=False, writemap=True)

    def load(self, filename):
        huge_dict = {}
        with open(filename, "r") as fh:
            for line in fh:
                js = json.loads(line)
                key = js["id"].rsplit("/", 1)[-1]
                val = js["parsed_name"]
                huge_dict[key] = val
        huge_dict = dict(sorted(huge_dict.items()))
        db = self.get_storage()
        db.update(huge_dict)


class GlobalIndexLoader(LmdbIndexLoader):
    def get_storage(self):
        mapExp = self.config.get("mapSizeExponent", 30)

        diff_path = self.config.get("differentDbPath", None)
        if diff_path:
            index = TabLmdb.open(diff_path, "c", map_size=2**mapExp, readahead=False, writemap=True)
        else:
            index = None

        if self.inverse_path:
            eqindex = TabLmdb.open(self.inverse_path, "c", map_size=2**mapExp, readahead=False, writemap=True)
        else:
            eqindex = None
        return (index, eqindex)

    def clear(self, which):
        (diffindex, eqindex) = self.get_storage()
        if which == "equivs":
            eqindex.clear()
        elif which == "diffs":
            diffindex.clear()
        else:
            print(f"Unknown index to clear {which}; should be equivs or diffs")

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

    def load(self, filename, which="equivs"):
        (diffindex, eqindex) = self.get_storage()

        if diffindex is None and eqindex is None:
            print(f"{self.name} has no indexes configured")
            return None
        elif not os.path.exists(filename):
            print(f"{filename} does not exist to load")
            return None
        elif not which in ["equivs", "diffs"]:
            print(f"{which} is not equivs or diffs")
            return None

        # Don't clear, as we'll call this multiple times
        n = 0
        updates = {}
        with open(filename) as csvfh:
            rdr = csv.reader(csvfh, delimiter=",")
            x = 0
            for row in rdr:
                if not row[0].startswith("http") or not row[1].startswith("http"):
                    continue
                (a, b) = row[:2]

                if a.startswith("https://lux.collections.yale.edu/data/"):
                    # Don't do this!
                    raise ValueError(f"File {filename} has LUX URI: {a}")
                else:
                    a2 = self.configs.canonicalize(a)

                if b.startswith("https://lux.collections.yale.edu/data/"):
                    # Don't do this!
                    raise ValueError(f"File {filename} has LUX URI: {b}")
                else:
                    b2 = self.configs.canonicalize(b)

                if a2 is None:
                    print(f"Got None for {a}")
                elif b2 is None:
                    print(f"Got None for {b}")
                else:
                    # NOTE: This doesn't have class
                    try:
                        if not b in updates[a]:
                            updates[a].append(b)
                    except:
                        updates[a] = [b]
                    try:
                        if not a in updates[b]:
                            updates[b].append(a)
                    except:
                        updates[b] = [a]

        if updates:
            if which == "equivs":
                eqindex.update(updates)
            else:
                diffindex.update(updates)
