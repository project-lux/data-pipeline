import os
import sys
import csv
from lux_pipeline.process.base.index_loader import IndexLoader


class GlobalIndexLoader(IndexLoader):
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
        diffindex = self.config.get("indexes", {}).get("differents", {}).get("index", None)
        eqindex = self.config.get("indexes", {}).get("equivalents", {}).get("index", None)

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
                    raise ValueError(f"File {csvfn} has LUX URI: {a}")
                else:
                    a2 = self.configs.canonicalize(a)

                if b.startswith("https://lux.collections.yale.edu/data/"):
                    # Don't do this!
                    raise ValueError(f"File {csvfn} has LUX URI: {b}")
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
