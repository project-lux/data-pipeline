
import os
import time
import gzip
import tarfile
import json
import zipfile

from pipeline.process.base.loader import Loader

class AshmolLoader(Loader):

    def __init__(self, config):
        self.in_path = config['dumpFilePath']
        self.out_cache = config['datacache']
        self.total = config.get('totalRecords', -1)
        self.force_reload = config.get('forceReload', False)

    def get_identifier_json(self, js):
        return js['id'].replace('https://data.ashmus.ox.ac.uk/ees/', '')

    def post_process_json(self, js):
        return js

    def filter_line(self, line):
        return False

    def load(self, slicen=None, maxSlice=None):
        # in is a directory of tgz files
        try:
            files = os.listdir(self.in_path)
            files.sort()
        except:
            files = [self.in_path]

        x = 0 
        done_x = 0
        start = time.time()
        for f in files:
            if not f.endswith('gz') and not f.endswith('zip'):
                continue

            if '/' in f:
                pfx = f.split('/')[-1]
                pfx = pfx.split('.')[0]
            else:
                pfx = f.split('.')[0]
                f = os.path.join(self.in_path, f)

            if f.endswith('gz'):
                tf = tarfile.open(f, "r:gz")
                members = tf.getmembers()
            else:
                tf = zipfile.ZipFile(f)
                members = tf.namelist()

            for ti in members:
                if type(ti) == str:
                    if ti.endswith('json') and "/" in ti:
                        bio = tf.open(ti)
                        ident = ti
                    else:
                        continue
                else:
                    if ti.name.endswith('json') and "/" in ti.name:
                        bio = tf.extractfile(ti)
                        ident = ti.name
                    else:
                        continue

                l = bio.read()
                try:
                    bio.close()
                except:
                    pass
                if len(l) < 30:
                    # Empty record means was previously deleted
                    continue

                # uri = base_uri + ti.name
                # what = f"{pfx}/{ident}"
                
                what = ident
                if not self.force_reload and what and what in self.out_cache:
                    done_x += 1
                    if not done_x % 10000:
                        print(f"Skipping past {done_x} {time.time() - start}")
                    continue
                # Cache assumes JSON as input, so need to parse it
                try:
                    js = json.loads(l) 
                except Exception as e:
                    l = l.replace(b'"value": .', b'"value": 0.')
                    try:
                        js = json.loads(l)
                    except Exception as e:
                        print(f"REALLY Broken record {ident} in {f}: {e}")
                        continue   
                x += 1
                self.out_cache[what] = js
                if not x % 10000:
                    t = time.time() - start
                    xps = x/t
                    ttls = 4000000 / xps
                    print(f"{x} in {t} = {xps}/s --> {ttls} total ({ttls/3600} hrs)")
            tf.close()
        self.out_cache.commit()