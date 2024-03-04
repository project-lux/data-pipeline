from pipeline.process.base.fetcher import Fetcher
import os, sys
import sqlite3
import json

class WofFetcher(Fetcher):
    

    def __init__(self, config):
        Fetcher.__init__(self, config)
        self.dumpdb = config['dumpFilePath']

    def make_fetch_uri(self, identifier):
        identifier = identifier.replace('.geojson', '')
        if '/' in identifier:
            return f"https://data.whosonfirst.org/{identifier}.geojson"
        else:
            # base /chunks-of-up-to-3/ pid.geojson
            chunks = []
            npid = identifier
            while npid:
                if len(npid) > 3:
                    chunks.append(npid[:3])
                    npid = npid[3:]
                else:
                    chunks.append(npid)
                    npid = ''
            return f"https://data.whosonfirst.org/{'/'.join(chunks)}/{identifier}.geojson"

    def fetch(self, identifier):
        # first check if we have the sqlite db
        fn = self.dumpdb
        if os.path.exists(fn):
            conn = sqlite3.connect(f"file:{fn}?mode=ro", uri=True)
            cursor = conn.cursor()
            if '/' in identifier:
                identifier = identifier.rsplit('/', 1)[1]
            ident = identifier.replace('.geojson', '')
            cursor.execute("SELECT body FROM geojson WHERE id=?", (ident, ))
            res = cursor.fetchall()
            try:
                jstr = res[0][0]
            except:
                # Asked for something we don't have...
                # pass to network
                return Fetcher.fetch(self, identifier)
            if type(jstr) == str:
                js = json.loads(jstr)
            else:
                js = jstr
            return {'data': js, 'source': self.name, 'identifier': identifier}

        else:
            return Fetcher.fetch(self, identifier)