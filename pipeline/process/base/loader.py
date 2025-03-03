
import io
import os
import requests
import shutil
import time
import gzip
import zipfile
import tarfile
import ujson as json
import tqdm

try:
    import magic
except:
    # Doesn't work ootb on mac (brew install libmagic)
    # or on windows
    magic = None

"""
type is:
    (container/])*format

Where container is one of:
  dir = directory of files
  dirh = directory hierarchy of files
  pair = pairtree directory hierarchy
  zip = zipfile of members
  tar = tarfile of members
  lines = single file where each line is a member
  dict = json file with a top level dictionary where each value is a member
  array = json file with a top level array where each item is a member
Format is one of:
  json = json, duh
  jsonstr = json but serialized to a string
  other = some non json format
And compression, on anything apart from dir and zip is one of:
  gz = gzipped
  bz = bzip2'd

dir/tar.gz/lines.bz/json = directory of tgz files, each entry is a jsonl file compressed by bzip2
"""



class NewLoader:

    def __init__(self, config):
        self.config = config
        self.configs = config['all_configs']
        self.out_cache = config.get('datacache', {})
        self.mapper = config.get('mapper', None)
        self.total = config.get('totalRecords', -1)
        self.my_slice = 0
        self.max_slice = 0
        self.seen = 0
        self.my_files = []
        self.temp_file_handles = {}
        self.progress_bar = None

        self.fmt_containers = ['dir', 'dirh', 'pair', 'zip', 'tar', 'lines', 'dict', 'array']
        self.fmt_formats = ['json', 'raw', 'other']
        self.fmt_compressions = ['gz', 'bz2']
        self.step_functions = {
            'dir': self.iterate_directory,
            'dirs': self.iterate_directories,
            'pair': self.iterate_directories,
            'zip': self.iterate_zip,
            'tar': self.iterate_tar,
            'lines': self.iterate_lines,
            'dict': self.iterate_dict,
            'array': self.iterate_array,
            'json': self.make_json,
            'raw': self.make_raw,
            'other': self.make_other
        }

    def guess_fmt(self, path):
        # FIXME: This needs more work...

        spec = []
        container = None
        compression = None
        fmt = None

        if (isdir := os.path.isdir(path)):
            # A directory ... of what?
            # If all we have is a directory, assume everything is consistent
            files = os.listdir(path)
            for f in files:
                if len(f) == 2 and os.path.isdir(os.path.join(path, f)):
                    container = 'pair'
                    break
                elif '.' in f and os.path.isfile(os.path.join(path, f)):
                    # recurse
                    sub = self.guess_fmt(os.path.join(path, f))
                    if sub:
                        # got something, otherwise keep looking
                        spec = [['dir'], sub]
                        break
        else:
            # A file ... what sort?
            # Trust extension to start

            if path.endswith('.gz'):
                compression = 'gz'
                path = path[:-3]
            elif path.endswith('.bz2'):
                compression = 'bz'
                path = path[:-4]

            if path.endswith('.zip'):
                container = 'zip'
            elif path.endswith('.tar'):
                container = 'tar'
            elif path.endswith('.tgz'):
                compression = 'gz'
                container = 'tar'
            elif path.endswith('.json'):
                # dunno what this is yet
                # FIXME: figure out if dict, array, lines, single file?
                fmt = 'json'
            elif path.endswith('.jsonl'):
                container = 'lines'
                fmt = 'json'

        if not spec and (container or fmt):
            if container:
                spec = [container]
            else:
                spec = [fmt]
            if compression:
                spec.append(compression)
        return [spec]


    def process_fmt(self, fmt):
        spec = []
        bits = fmt.split('/')
        fmt = bits.pop(-1)
        for b in bits:
            bb = b.split('.')
            if not bb[0] in self.fmt_containers:
                raise ValueError(f"Cannot process container type {bb[0]} in {self.config['name']} loader")
            if len(bb) == 2 and bb[1] not in self.fmt_compressions:
                raise ValueError(f"Cannot process compression type {bb[1]} in {self.config['name']} loader")
            if len(bb) > 2:
                raise ValueError(f"Badly specified container: {bb} in {self.config['name']} loader")
            spec.append(bb)
        fmts = fmt.split('.')
        if not fmts[0] in self.fmt_formats:
            raise ValueError(f"Cannot process format type {fmts[0]} in {self.config['name']} loader")
        if len(fmts) == 2 and fmts[1] not in self.fmt_compressions:
            raise ValueError(f"Cannot process compression type {fmts[1]} in {self.config['name']} loader")
        if len(fmts) > 2:
            raise ValueError(f"Badly specified container: {fmts} in {self.config['name']} loader")
        spec.append(fmts)
        return spec


    def iterate_directory(self, path, comp):
        # ignore comp
        for f in os.listdir(path):
            full = os.path.join(path, f)
            if os.path.isfile(full):
                yield full

    def iterate_directories(self, path, comp):
        # still ignore comp
        for f in os.listdir(path):
            full = os.path.join(path, f)
            if os.path.isfile(full):
                yield full
            else:
                self.iterate_directories(full, comp)

    def iterate_zip(self, path, comp):
        if comp == 'bz2':
            compression = zipfile.ZIP_BZIP2
        elif comp == 'gz':
            compression = zipfile.ZIP_DEFLATED
        else:
            compression = zipfile.ZIP_STORED

        with zipfile.ZipFile(path, compression=compression) as zh:
            for n in zh.namelist():
                if not n.endswith('/'):
                    # can't get back to this, so need to yield a file handle like object
                    yield zh.open(n)

    def iterate_tar(self, path, comp):
        if comp:
            mode = f"r:{comp}"
        else:
            mode = "r"
        with tarfile.open(path, mode) as th:
            ti = th.next()
            while ti is not None:
                if ti.isfile():
                    yield th.extractfile(ti)
                ti = th.next()

    def file_opener(self, path, comp):
        if not comp and isinstance(path, io.IOBase):
            # already a file handle
            return path
        if comp == 'gz':
            return gzip.open(path)
        elif comp == 'bz2':
            return bz2.open(path)
        elif not comp:
            return open(path)
        else:
            # Dunno what this is
            return None

    def iterate_lines(self, path, comp):
        with self.file_opener(path, comp) as fh:
            l = fh.readline()
            while l:
                if type(l) == str:
                    yield io.StringIO(l)
                elif type(l) == bytes:
                    yield io.BytesIO(l)
                l = fh.readline()

    def iterate_dict(self, path, comp):
        with self.file_opener(path, comp) as fh:
            data = json.load(fh)
            for v in data.values():
                yield v

    def iterate_array(self, path, comp):
        with self.file_opener(path, comp) as fh:
            data = json.load(fh)
            # This is yield actual json, not a file/string of json
            for v in data:
                yield v

    def make_raw(self, path, comp, parent):
        # path is the actual native JSON record
        data = self.post_process_json(path)
        ident = self.extract_identifier(data)
        if not ident:
            raise ValueError(f"Could not get an identifier in {self.config['name']} while in {parent}")
        return {'identifier': ident, 'data': data}

    def make_json(self, path, comp, parent):
        ident = self.make_identifier(path)
        with self.file_opener(path, comp) as fh:
            data = json.load(fh)
        data = self.post_process_json(data)
        if not ident:
            ident = self.extract_identifier(data)
            if not ident:
                raise ValueError(f"Could not get an identifier in {self.config['name']} while in {parent}/{path}")
        return {'identifier': ident, 'data': data}

    def make_other(self, path, comp, parent):
        ident = self.make_identifier(path)
        with self.file_opener(path, comp) as fh:
            data = fh.read()
        data = self.post_process_other(data)
        if not type(data) == dict:
            data = {'data': data}
        if not ident:
            ident = self.extract_identifier(data)
            if not ident:
                raise ValueError(f"Could not get an identifier in {self.config['name']} while in {parent}/{path}")
        return {'identifier': ident, 'data': data}

    def make_identifier(self, value):
        # assume a filepath with the last component as the identifier
        if hasattr(value, 'name'):
            value = value.name
        try:
            return value.split('/')[-1]
        except:
            return None

    def extract_identifier(self, data):
        # Could be anywhere, but at least check 'id'
        if type(data) == dict and 'id' in data:
            return self.make_identifier(data['id'])
        return None

    def post_process_json(self, data):
        # This is called after discovering JSON and before extracting identifier
        return data

    def post_process_other(self, data):
        # This is called after discovering the record and before extracting the identifier
        return data

    def should_make_record(self, path):
        if self.max_slice > 1 and self.seen % self.max_slice != self.my_slice:
            return False
        return True

    def should_store_record(self, data):
        return True

    def post_store_record(self, record):
        # This is called after successfully storing the record
        # Handle index data extraction here
        entries = 0
        if self.temp_file_handles:
            fields = self.mapper.extract_index_data(record)
            for (f, vals) in fields.items():
                if f in self.temp_file_handles:
                    for (a,b) in vals:
                        valstr = f"{a}\t{b}\n"
                        self.temp_file_handle[f].write(valstr)
                        entries += 1
        return entries

    def open_temp_files(self):
        if 'reconcileDbPath' in self.config:
            lblfn = os.path.join(self.configs.temp_dir, f"{self.config['name']}_labels_{self.my_slice}.tsv")
            lbl = open(llblfn, 'w')
            self.temp_file_handles['label'] = lbl
        if 'inverseEquivDbPath' in self.config:
            eqfn = os.path.join(self.configs.temp_dir, f"{self.config['name']}_equivs_{self.my_slice}.tsv")
            eq = open(eqfn, 'w')
            self.temp_file_handles['equiv'] = eq
        if 'hasDifferentFrom' in self.config:
            diffn = os.path.join(self.configs_temp_dir, f"{self.config['name']}_diffs_{self.my_slice}.tsv")
            diff = open(diffn, 'w')
            self.temp_file_handles['diff'] = diff


    def close_temp_files(self):
        for t in ['label', 'equiv', 'diff']:
            if t in self.temp_file_handles and (fh := self.temp_file_handles[t]) and not fh.closed:
                fh.close()

    def store_record(self, record):
        identifier = record['identifier']
        data = record['data']
        try:
            self.out_cache[identifier] = data
        except:
            return False
        if self.progress_bar is not None:
            self.progress_bar.update(1)
        return True

    def process_step(self, steps, path, parent):
        step = steps[0]
        comp = step[1] if len(step) > 1 else None
        handler = self.step_functions[step[0]]
        if step[0] in self.fmt_containers:
            for child in handler(path, comp):
                self.process_step(steps[1:], child, step)
        elif step[0] in self.fmt_formats:
            # if we don't need to process it, then don't
            self.seen += 1
            if self.should_make_record(path):
                record = handler(path, comp, parent)
                if self.should_store_record(record):
                    okay = self.store_record(record)
                    if okay:
                        self.post_store_record(record)

        else:
            raise ValueError(f"Unknown step type {step} in {self.config['name']}")

    def open_progress_bar(self):
        ttl = self.total
        if self.max_slice > 1:
            ttl = ttl // self.max_slice
        self.progress_bar = tqdm.tqdm(total=ttl,
            desc=f'{self.config['name']}/{self.my_slice}',
            position=self.my_slice,
            leave=True)

    def close_progress_bar(self):
        if self.progress_bar is not None:
            self.progress_bar.close()

    def prepare_load(self, my_slice=0, max_slice=0):
        self.my_slice = my_slice
        self.max_slice = max_slice

        files = []
        if (ifs := self.config.get('input_files', {})):
            for p in ifs['records']:
                fmt = p.get('type', None)
                if fmt:
                    fmtspec = self.process_fmt(fmt)
                else:
                    # gotta guess
                    fmtspec = self.guess_fmt(p['path'])
                files.append({"path": p['path'], "fmt": fmtspec})

        if not files and (dfp := self.config.get('dumpFilePath')):
            # look in dfp
            fmt = self.config.get('dumpFileType', None)
            if fmt:
                fmtspec = self.process_fmt(fmt)
            else:
                # Guessing again
                fmtspec = self.guess_fmt(dfp)
            files.append({"path": dfp, "fmt": fmtspec})
        self.my_files = files


    def load(self, disable_tqdm=False):

        self.open_temp_files()
        for info in self.my_files:
            if not disable_tqdm:
                self.open_progress_bar()
            self.process_step(info['fmt'], info['path'], None)
            if not disable_tqdm:
                self.close_progress_bar()
        try:
            self.out_cache.commit()
        except:
            pass
        self.close_temp_files()


class Loader(object):

    def __init__(self, config):
        self.config = config
        self.in_url = config.get('remoteDumpFile', '')
        self.in_path = config.get('dumpFilePath', '')
        self.out_cache = config['datacache']
        self.total = config.get('totalRecords', -1)

    def get_identifier_raw(self, line):
        # Find identifier from raw line
        return None

    def get_identifier_json(self, js):
        return None

    def post_process_json(self, js):
        return js

    def filter_line(self, line):
        return False

    def load(self):
        # default is to assume gzipped JSONL
        # without headers/footers or other wrapping
        # Dump in raw without parsing

        if self.in_path.endswith('.gz'):
            fh = gzip.open(self.in_path)
        elif self.in_path.endswith('.zip'):
            zh = zipfile.ZipFile(self.in_path)
            # Assume a single zipped file
            names = zh.namelist()
            if len(names) != 1:
                raise ValueError("Too many zipped files")
            else:
                fh = zh.open(names[0])

        start = time.time()
        x = 0 
        done_x = 0
        l = 1
        while l:
            l = fh.readline()
            if not l:
                break
            # Find id and check if already exists before processing JSON
            what = self.get_identifier_raw(l)
            if what and what in self.out_cache:
                done_x += 1
                if not done_x % 10000:
                    print(f"Skipping past {done_x} {time.time() - start}")
                continue
            # Cache assumes JSON as input, so need to parse it
            try:
                js = json.loads(l)
            except:
                raise    
            x += 1
            try:
                new = self.post_process_json(js)
            except:
                print(f"Failed to process {l}")
                raise
            if new is not None:
                if not what:
                    what = self.get_identifier_json(new)
                    if not what:
                        print(l)
                        raise NotImplementedError(f"is get_identifier_raw or _json implemented for {self.__class__.__name__}?")
                self.out_cache[what] = new
            if not x % 10000:
                t = time.time() - start
                xps = x/t
                ttls = self.total / xps
                print(f"{x} in {t} = {xps}/s --> {ttls} total ({ttls/3600} hrs)")
        fh.close()
        self.out_cache.commit()

    def load_export(self):
        where = self.configs.dumps_dir
        zipfn = os.path.join(where, f'export_{self.config["name"]}.zip')
        if not os.path.exists(zipfn):
            zipfn = os.path.join(where, f'{self.config["name"]}.zip')
        if os.path.exists(zipfn):
            zh = zipfile.ZipFile(zipfn, 'r', compression=zipfile.ZIP_BZIP2)
        else:
            print("Could not find export zip")
            return None
        start = time.time()
        x = 0
        idents = zh.namelist()
        total = len(idents)
        print(total)
        for ident in idents:
            fh = zh.open(ident)
            data = fh.read()
            fh.close()
            try:
                data = data.decode('utf-8')
                js = json.loads(data)
            except Exception as e:
                print(e)
                continue
            x += 1
            self.out_cache[ident] = js['data']
            if not x % 10000:
                t = time.time() - start
                xps = x/t
                ttls = total / xps
                print(f"{x} in {t} = {xps}/s --> {ttls} total ({ttls/3600} hrs)")
        zh.close()



