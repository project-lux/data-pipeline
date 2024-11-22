from pipeline.process.utils.mapper_utils import get_year_from_timespan
from sqlitedict import SqliteDict
from pipeline.storage.idmap.lmdb import TabLmdb


# Abstract class definition, useless without actual data
class Reconciler(object):
    def __init__(self, config):
        self.config = config
        self.namespace = config["namespace"]
        self.configs = config["all_configs"]
        self.debug = config["all_configs"].debug_reconciliation
        self.debug_graph = {}

    def should_reconcile(self, rec, reconcileType="all"):
        if "data" in rec:
            data = rec["data"]
        if self.name_index is None and reconcileType == "name":
            return False
        elif self.id_index is None and reconcileType == "uri":
            return False
        elif "identified_by" not in data and reconcileType == "name":
            # record without a name should never happen ... but ...
            return False
        elif "equivalent" not in data and reconcileType == "uri":
            return False
        elif "source" in rec and rec["source"] == self.config["name"]:
            # Don't reconcile against self
            return False
        elif (
            self.name_index is None and self.id_index is None and reconcileType == "all"
        ):
            return False
        return True

    def reconcile(self, record, reconcileType="all"):
        identifier = None
        if self.should_reconcile(record):
            # does entity exist in this dataset?
            pass
        return identifier

    def extract_uris(self, rec):
        equivs = rec.get("equivalent", [])
        return [x["id"] for x in equivs if "id" in x]

    def extract_names(self, rec):
        ns = self.configs.external["aat"]["namespace"]
        gbls = self.configs.globals_cfg
        aat_primaryName = ns + gbls["primaryName"]

        check_langs = {
            ns + gbls["lang_en"]: 1,  # 820,063
            ns + gbls["lang_fr"]: 2,  # 308,196
            ns + gbls["lang_nl"]: 3,  # 307,119
            ns + gbls["lang_de"]: 4,  # 304,000
            ns + gbls["lang_es"]: 5,  # 259,160
            ns + gbls["lang_ar"]: 6,  # 184,612
            ns + "300389115": 7,  # 172,184
            ns + gbls["lang_zh"]: 8,  # 148,055
        }

        # FIXME: These should be globals!
        aat_firstName = "http://vocab.getty.edu/aat/300404651"
        aat_middleName = "http://vocab.getty.edu/aat/300404654"
        aat_lastName = "http://vocab.getty.edu/aat/300404652"

        vals = {}
        typ = rec["type"]
        nms = rec.get("identified_by", [])
        for nm in nms:
            langs = nm.get("language", [])
            langids = [lg.get("id", None) for lg in langs]
            if None in langids:
                print(f"  None in Name language: {rec['id']}")

            cxns = nm.get("classified_as", [])
            cxnids = [cx.get("id", None) for cx in cxns]
            if None in cxnids:
                print(f"  None in Name classifications: {rec['id']}")

            if aat_primaryName in cxnids and "content" in nm:
                val = nm["content"]
                for lang_id, num in check_langs.items():
                    if lang_id in langids:
                        val = nm["content"].lower().strip()
                        vals[val] = num
                    else:
                        vals[val] = 3

                if typ == "Person":
                    if "part" in nm:
                        parts = nm["part"]
                        if not type(parts) == list:
                            parts = [parts]
                        first = ""
                        middle = ""
                        last = ""
                        for part in parts:
                            cxns = part.get("classified_as", [])
                            if aat_firstName in [cx["id"] for cx in cxns]:
                                first = part["content"].lower().strip()
                            elif aat_middleName in [cx["id"] for cx in cxns]:
                                middle = part["content"].lower().strip()
                            elif aat_lastName in [cx["id"] for cx in cxns]:
                                last = part["content"].lower().strip()

                        if last and first and middle:
                            vals[f"{last}, {first} {middle}"] = 1
                            break
                        elif last and first:
                            vals[f"{last}, {first}"] = 1
                elif typ == "Place" and "--" in val:
                    val1, val2 = val.split("--", 1)
                    vals[f"{val2} ({val1})"] = 1
                    vals[f"{val1} ({val2})"] = 1

        if typ == "Person":
            birth = get_year_from_timespan(rec.get("born", {}))
            death = get_year_from_timespan(rec.get("died", {}))
            if birth or death:
                for v in vals.keys():
                    if birth and birth not in v:
                        vals[f"{v}, {birth}-"] = 1
                    if death and death not in v:
                        vals[f"{v}, -{death}"] = 1
                    if birth and death and birth not in v and death not in v:
                        vals[f"{v}, {birth}-{death}"] = 1

            # FIXME Out of pipeline:
            # subst out b. if after two ,s and followed by numbers
            # e.g. pierce, e. dana (edwin dana), b. 1871
            # but not jones, fred b., 1871-
            # for v in vals[:]:
            #    if v.endswith('-'):
            #        # Check if there's v with a death date
            #        plus_deaths = self.get_keys_like('name', v)
            #        if len(plus_deaths) == 1:
            #            vals.append(plus_deaths[0])
            #            print(f" %%% Found +death for {rec['data']['id']}")
        return vals


# Lightning Memory-Mapped DB Reconciler


class LmdbReconciler(Reconciler):
    def __init__(self, config):
        Reconciler.__init__(self, config)

        fn = config.get("reconcileDbPath", "")
        fn2 = config.get("inverseEquivDbPath", "")
        try:
            if fn:
                self.name_index = TabLmdb.open(fn, "r", readahead=False, writemap=True)
            else:
                self.name_index = None
        except Exception:
            self.name_index = None
        try:
            if fn2:
                self.id_index = TabLmdb.open(fn2, "r", readahead=False, writemap=True)
            else:
                self.id_index = None
        except Exception:
            self.id_index = None

    def get_keys_like(self, which, key):
        # Use set_range()
        pass

    def reconcile(self, rec, reconcileType="all"):
        # Match by primary name
        if not reconcileType in ["all", "name", "uri"]:
            return None
        if not self.should_reconcile(rec, reconcileType):
            return None
        if "data" in rec:
            rec = rec["data"]

        matches = {}
        my_type = rec["type"]

        if reconcileType in ["all", "name"]:
            # Get name from Record
            vals = self.extract_names(rec)
            vals = dict(sorted(vals.items(), key=lambda item: (item[1], -len(item[0]))))
            for nm, num in vals.items():
                if nm in self.name_index:
                    try:
                        (k, typ) = self.name_index[nm]
                    except Exception:
                        k = self.name_index[nm]
                        typ = None
                    if typ is not None and my_type == typ:
                        if self.debug:
                            recid = rec.get("id", "")
                            if recid:
                                try:
                                    self.debug_graph[rec["id"]].append(
                                        (f"{self.namespace}{k}", "nm")
                                    )
                                except Exception:
                                    self.debug_graph[rec["id"]] = [
                                        (f"{self.namespace}{k}", "nm")
                                    ]
                        try:
                            matches[k].append(nm)
                            if num > 1:
                                print(
                                    f"!!!Base reconciler matched a non-English name: {nm}, with lang value {num}"
                                )
                        except Exception:
                            matches[k] = [nm]
                        break

        if reconcileType in ["all", "uri"]:
            for e in self.extract_uris(rec):
                if e in self.id_index:
                    (uri, typ) = self.id_index[e]
                    if my_type != typ and self.debug:
                        print(
                            f"cross-type match: record has {my_type} and external has {typ}"
                        )
                    try:
                        matches[uri].append(e)
                    except Exception:
                        matches[uri] = [e]
                    if self.debug:
                        try:
                            self.debug_graph[e].append(
                                (f"{self.namespace}{uri}", "uri")
                            )
                        except Exception:
                            self.debug_graph[e] = [(f"{self.namespace}{uri}", "uri")]

        if len(matches) == 1:
            return f"{self.namespace}{list(matches.keys())[0]}"
        elif matches:
            ms = list(matches.items())
            ms.sort(key=lambda x: len(x))
            ms.reverse()
            if self.debug:
                print(f"Found multiple matches: {ms}")
            return f"{self.namespace}{ms[0][0]}"
        else:
            return None


# Old SQLite Reconciler
class SqliteReconciler(Reconciler):
    def __init__(self, config):
        Reconciler.__init__(self, config)

        fn = config.get("reconcileDbPath", "")
        fn2 = config.get("inverseEquivDbPath", "")
        try:
            if fn:
                self.name_index = SqliteDict(fn, autocommit=False, flag="r")
            else:
                self.name_index = None
        except:
            # Can't get a lock, set to None
            self.name_index = None
        try:
            if fn2:
                self.id_index = SqliteDict(fn2, autocommit=False, flag="r")
            else:
                self.id_index = None
        except Exception:
            # Can't get a lock, set to None
            self.id_index = None

    def get_keys_like(self, which, key):
        # WARNING: This is HORRIBLY SLOW in Sqlite compared to LMDB
        if which == "name":
            idx = self.name_index
        elif which == "id":
            idx = self.id_index
        else:
            raise ValueError(f"Unknown sqlitedict type {which}")
        QUERY = 'SELECT key FROM "%s" WHERE key LIKE ?' % idx.tablename
        items = idx.conn.select(QUERY, (f"{key}%",))
        return [x[0] for x in items]

    def reconcile(self, rec, reconcileType="all"):
        # Match by primary name
        if reconcileType not in ["all", "name", "uri"]:
            return None
        if not self.should_reconcile(rec, reconcileType):
            return None
        if "data" in rec:
            rec = rec["data"]

        matches = {}
        my_type = rec["type"]

        if reconcileType in ["all", "name"]:
            # Get name from Record
            vals = self.extract_names(rec)
            vals = sorted(vals.items(), key=lambda item: (item[1], -len(item[0])))
            vals = [x[0] for x in vals]
            for val in vals:
                if val in self.name_index:
                    try:
                        (k, typ) = self.name_index[val]
                    except Exception:
                        k = self.name_index[val]
                        typ = None
                    if typ is not None and my_type == typ:
                        try:
                            matches[k].append(val)
                        except Exception:
                            matches[k] = [val]
                        break

        if reconcileType in ["all", "uri"]:
            for e in self.extract_uris(rec):
                if e in self.id_index:
                    (uri, typ) = self.id_index[e]
                    if my_type != typ:
                        if self.debug:
                            print(
                                f"cross-type match: record has {my_type} and external has {typ}"
                            )
                    try:
                        matches[uri].append(e)
                    except:
                        matches[uri] = [e]
        if len(matches) == 1:
            return f"{self.namespace}{list(matches.keys())[0]}"
        elif matches:
            ms = list(matches.items())
            ms.sort(key=lambda x: len(x))
            ms.reverse()
            if self.debug:
                print(f"Found multiple matches: {ms}")
            return f"{self.namespace}{ms[0][0]}"
        else:
            return None
