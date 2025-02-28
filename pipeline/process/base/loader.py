
import os
import requests
import shutil
import time
import gzip
import zipfile
import ujson as json

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
        self.out_cache = config['datacache']
        self.total = config.get('totalRecords', -1)
        self.my_slice = 0
        self.max_slice = 0
        self.my_files = []
        self.fmt_containers = ['dir', 'dirh', 'pair', 'zip', 'tar', 'lines', 'dict', 'array']
        self.fmt_formats = ['json', 'jsonstr', 'other']
        self.fmt_compressions = ['gz', 'bz']

        self.step_functions = {
            'dir': self.iterate_directory,
            'dirh': self.iterate_directories,
            'pair': self.iterate_directories,
            'zip': self.iterate_zip,
            'tar': self.iterate_tar,
            'lines': self.iterate_lines,
            'dict': self.iterate_dict,
            'array': self.iterate_array,
            'json': self.process_json,
            'jsonstr': self.process_jsonstr,
            'other': self.process_other
        }

    def guess_fmt(self, path):
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
        return spec


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


    def prepare_for_load(self, my_slice=0, max_slice=0):
        self.my_slice = my_slice
        self.max_slice = max_slice

        files = []
        if (ifs := self.config.get('input_files', {})):
            for p in self.in_paths['records']:
                fmt = p.get('type', None)
                if fmt:
                    fmtspec = self.process_fmt(fmt)
                else:
                    # gotta guess
                    fmtspec = self.guess_fmt(p['path'])
                files.append({"path": p['path'], "fmt": fmtspec})

        if not files and (dfp := self.config.get('dumpFilePath')):
            # look in dfp
            fmt = config.get('dumpFileType', None)
            if fmt:
                fmtspec = self.process_fmt(fmt)
            else:
                # Guessing again
                fmtspec = self.guess_fmt(dfp)
            files.append({"path": dfp, "fmt": fmtspec})
        self.my_files = files



    def iterate_directory(self, path, comp):
        pass

    def iterate_directories(self, path, comp):
        pass

    def iterate_zip(self, path, comp):
        pass

    def iterate_tar(self, path, comp):
        pass

    def iterate_lines(self, path, comp):
        pass

    def iterate_dict(self, path, comp):
        pass

    def iterate_array(self, path, comp):
        pass



    def process_step(self, steps, path, parent):
        step = steps[0]
        comp = step[1] if len(step) > 1 else None
        handler = self.step_functions[step[0]]
        if step[0] in self.fmt_containers:
            for child in handler(path, comp):
                self.process_step(steps[1:], child, step)
        elif step[0] in self.fmt_formats:
            handler(path, comp, parent)
        else:
            raise ValueError(f"Unknown step type {step} in {self.config['name']}")

    def make_identifier(self, value):
        # assume a filepath with the last component as the identifier
        return value.split('/')[-1]


    def load(self):

        for info in self.my_files:
            path = info['path']
            fmt = info['type']
            self.process_step(steps, path, None)



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
        where = self.config['all_configs'].dumps_dir
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



