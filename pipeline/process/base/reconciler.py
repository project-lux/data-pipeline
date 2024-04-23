

from pipeline.process.utils.mapper_utils import get_year_from_timespan
from sqlitedict import SqliteDict
from lmdbm import Lmdb


# Abstract class definition, useless without actual data
class Reconciler(object):

    def __init__(self, config):
        self.config = config
        self.namespace = config['namespace']
        self.configs = config['all_configs']
        self.debug = config['all_configs'].debug_reconciliation

    def should_reconcile(self, rec, reconcileType="all"):
        if 'data' in rec:
            rec = rec['data']
        if self.name_index is None and reconcileType == 'name':
            return False
        elif self.id_index is None and reconcileType == 'uri':
            return False            
        elif not 'identified_by' in rec and reconcileType == 'name':
            # record without a name should never happen ... but ...
            return False
        elif not 'equivalent' in rec and reconcileType == 'uri':
            return False
        elif self.name_index is None and self.id_index is None and reconcileType == 'all':
            return False
        return True

    def reconcile(self, record, reconcileType="all"):
        identifier = None
        if self.should_reconcile(record):
            # does entity exist in this dataset?
            pass
        return identifier

    def extract_uris(self, rec):
        equivs = rec.get('equivalent', [])
        return [x['id'] for x in equivs if 'id' in x]

    def extract_names(self, rec):
        aat_primaryName = self.configs.external['aat']['namespace'] + self.configs.globals_cfg['primaryName']
        # FIXME: These should be globals!
        aat_firstName = "http://vocab.getty.edu/aat/300404651"
        aat_middleName = "http://vocab.getty.edu/aat/300404654"
        aat_lastName = "http://vocab.getty.edu/aat/300404652"

        vals = []
        typ = rec['type']
        nms = rec.get('identified_by', [])
        for nm in nms:
            cxns = nm.get('classified_as', [])
            if aat_primaryName in [cx['id'] for cx in cxns] and 'content' in nm:
                val = nm['content'].lower().strip()
                vals.append(val)

                if typ == 'Person':
                    if 'part' in nm:
                        parts = nm['part']
                        if not type(parts) == list:
                            parts = [parts]
                        first = "" ; middle = "" ; last = ""
                        for part in parts:
                            cxns = part.get('classified_as',[])
                            if aat_firstName in [cx['id'] for cx in cxns]:
                                first = part['content'].lower().strip()
                            elif aat_middleName in [cx['id'] for cx in cxns]:
                                middle = part['content'].lower().strip()
                            elif aat_lastName in [cx['id'] for cx in cxns]:
                                last = part['content'].lower().strip()

                        if last and first:
                            vals.append(f"{last}, {first}")
                            if middle:
                                vals.append(f"{last}, {first} {middle}")
                elif typ == 'Place' and '--' in val:
                    val1, val2 = val.split("--", 1)
                    vals.append(f"{val2} ({val1})")
                    vals.append(f"{val1} ({val2})")

        if typ == 'Person':
            birth = get_year_from_timespan(rec.get('born',{}))
            death = get_year_from_timespan(rec.get('died',{}))
            if birth or death:
                for v in vals[:]:
                    if birth and not birth in v:
                        vals.append(f"{v}, {birth}-")
                    if death and not death in v:
                        vals.append(f"{v}, -{death}")
                    if birth and death and not birth in v and not death in v:
                        vals.append(f"{v}, {birth}-{death}")

            # FIXME Out of pipeline:
            # subst out b. if after two ,s and followed by numbers
            # e.g. pierce, e. dana (edwin dana), b. 1871
            # but not jones, fred b., 1871-
            #for v in vals[:]:
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
                self.name_index = read-only-lmdb-here for fn        
            else:
                self.name_index = None
        except:
            # Can't get a lock, set to None
            self.name_index = None
        try:
            if fn2:
                self.id_index = read-only-lmdb-here for fn2       
            else:
                self.id_index = None
        except:
            # Can't get a lock, set to None
            self.id_index = None

    def get_keys_like(self, which, key):
        # Use set_range()
        pass

    def reconcile(self, rec, reconcileType="all"):
        # Match by primary name
        if not reconcileType in ['all', 'name', 'uri']:
            return None
        if not self.should_reconcile(rec, reconcileType):
            return None
        if 'data' in rec:
            rec = rec['data']

        matches = {}
        my_type = rec['type']

        if reconcileType in ['all', 'name']:
            # Get name from Record
            vals = self.extract_names(rec)
            vals.sort(key=len,reverse=True)
            for val in vals:
                if val in self.name_index:
                    try:
                        (k, typ) = self.name_index[val]
                    except:
                        k = self.name_index[val]
                        typ = None
                    if typ is not None and my_type == typ:
                        try:
                            matches[k].append(val)
                        except:
                            matches[k] = [val]
                        break

        if reconcileType in ['all', 'uri']:
            for e in self.extract_uris(rec):
                if e in self.id_index:
                    (uri, typ) = self.id_index[e]
                    if my_type != typ:
                        if self.debug: print(f"cross-type match: record has {my_type} and external has {typ}")
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
            if self.debug: print(f"Found multiple matches: {ms}")            
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
                self.name_index = SqliteDict(fn, autocommit=False, flag='r')        
            else:
                self.name_index = None
        except:
            # Can't get a lock, set to None
            self.name_index = None
        try:
            if fn2:
                self.id_index = SqliteDict(fn2, autocommit=False, flag='r')        
            else:
                self.id_index = None
        except:
            # Can't get a lock, set to None
            self.id_index = None

    def get_keys_like(self, which, key):
        # WARNING: This is HORRIBLY SLOW in Sqlite compared to LMDB
        if which == 'name':
            idx = self.name_index
        elif which == 'id':
            idx = self.id_index
        else:
            raise ValueError(f"Unknown sqlitedict type {which}")
        QUERY = 'SELECT key FROM "%s" WHERE key LIKE ?' % idx.tablename
        items = idx.conn.select(QUERY, (f"{key}%",))
        return [x[0] for x in items]

    def reconcile(self, rec, reconcileType="all"):
        # Match by primary name
        if not reconcileType in ['all', 'name', 'uri']:
            return None
        if not self.should_reconcile(rec, reconcileType):
            return None
        if 'data' in rec:
            rec = rec['data']

        matches = {}
        my_type = rec['type']

        if reconcileType in ['all', 'name']:
            # Get name from Record
            vals = self.extract_names(rec)
            vals.sort(key=len,reverse=True)
            for val in vals:
                if val in self.name_index:
                    try:
                        (k, typ) = self.name_index[val]
                    except:
                        k = self.name_index[val]
                        typ = None
                    if typ is not None and my_type == typ:
                        try:
                            matches[k].append(val)
                        except:
                            matches[k] = [val]
                        break

        if reconcileType in ['all', 'uri']:
            for e in self.extract_uris(rec):
                if e in self.id_index:
                    (uri, typ) = self.id_index[e]
                    if my_type != typ:
                        if self.debug: print(f"cross-type match: record has {my_type} and external has {typ}")
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
            if self.debug: print(f"Found multiple matches: {ms}")            
            return f"{self.namespace}{ms[0][0]}"
        else:
            return None

       