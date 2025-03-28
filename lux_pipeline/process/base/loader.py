
import io
import os
import requests
import shutil
import time
import gzip
import bz2
import zipfile
import tarfile
import ujson as json
import logging
logger = logging.getLogger("lux_pipeline")
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
  zip = zipfile of members
  tar = tarfile of members
  lines = single file where each line is a member
  dict = json file with a top level dictionary where each value is a member
  array = json file with a top level array where each item is a member
  arraylines = json file, but each record is on a separate line
Format is one of:
  json = json, duh
  jsonstr = json but serialized to a string
  other = some non json format
And compression, on anything apart from dir and zip is one of:
  gz = gzipped
  bz = bzip2'd

dir/tar.gz/lines.bz/json = directory of tgz files, each entry is a jsonl file compressed by bzip2
datacache exports are "zip.bz2/json"

"""

class Pointer:
    def __init__(self, parent, info):
        self.parent = parent
        self.info = info

class TarPointer(Pointer):
    # parent is a TarFile
    # info is a TarInfo
    def get_name(self):
        return self.info.name
    def get_handle(self):
        return self.parent.extractfile(self.info)

class ZipPointer(Pointer):
    # parent is ZipFile
    # info is a string
    def get_name(self):
        return self.info
    def get_handle(self):
        return self.parent.open(self.info)


class Loader:

    def __init__(self, config):
        self.config = config
        self.name = config['name']
        self.configs = config['all_configs']
        self.out_cache = config.get('datacache', {})
        self.mapper = config.get('mapper', None)
        self.total = config.get('totalRecords', -1)
        self.increment_total = self.total < 0
        self.dumps_dir = config['all_configs'].dumps_dir
        if 'dumps_dir' in config:
            self.dumps_dir = os.path.join(self.dumps_dir, config['dumps_dir'])
        self.my_slice = 0
        self.max_slice = 0
        self.seen = 0
        self.my_files = []
        self.temp_file_handles = {}
        self.overwrite = False
        self.load_manager = None

        self.fmt_containers = ['dir', 'dirh', 'pair', 'zip', 'tar', 'lines', 'dict', 'array', 'arraylines']
        self.fmt_formats = ['json', 'raw', 'other']
        self.fmt_compressions = ['gz', 'bz2']
        self.step_functions = {
            'dir': self.iterate_directory,
            'dirs': self.iterate_directories,
            'pair': self.iterate_directories,
            'zip': self.iterate_zip,
            'tar': self.iterate_tar,
            'lines': self.iterate_lines,
            'arraylines': self.iterate_arraylines,
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
                raise ValueError(f"Cannot process container type {bb[0]} in {self.name} loader")
            if len(bb) == 2 and bb[1] not in self.fmt_compressions:
                raise ValueError(f"Cannot process compression type {bb[1]} in {self.name} loader")
            if len(bb) > 2:
                raise ValueError(f"Badly specified container: {bb} in {self.name} loader")
            spec.append(bb)
        fmts = fmt.split('.')
        if not fmts[0] in self.fmt_formats:
            raise ValueError(f"Cannot process format type {fmts[0]} in {self.name} loader")
        if len(fmts) == 2 and fmts[1] not in self.fmt_compressions:
            raise ValueError(f"Cannot process compression type {fmts[1]} in {self.name} loader")
        if len(fmts) > 2:
            raise ValueError(f"Badly specified container: {fmts} in {self.name} loader")
        spec.append(fmts)
        return spec


    def iterate_directory(self, path, comp, remaining):
        # ignore comp
        files = os.listdir(path)
        if self.increment_total and len(remaining) == 1:
            self.update_progress_bar(increment_total=len(files))
        for f in files:
            full = os.path.join(path, f)
            if os.path.isfile(full):
                yield full

    def iterate_directories(self, path, comp, remaining):
        # still ignore comp
        for f in os.listdir(path):
            full = os.path.join(path, f)
            if os.path.isfile(full):
                if self.increment_total and len(remaining) == 1:
                    self.update_progress_bar(increment_total=len(files))
                yield full
            else:
                self.iterate_directories(full, comp, remaining)

    def iterate_zip(self, path, comp, remaining):
        if comp == 'bz2':
            compression = zipfile.ZIP_BZIP2
        elif comp == 'gz':
            compression = zipfile.ZIP_DEFLATED
        else:
            compression = zipfile.ZIP_STORED

        with zipfile.ZipFile(path, compression=compression) as zh:
            names = zh.namelist()
            if self.increment_total and len(remaining) == 1:
                self.update_progress_bar(increment_total=len(names))
            for n in names:
                if not n.endswith('/'):
                    # can't get back to this, so need to yield a file handle like object
                    yield ZipPointer(zh, n)

    def iterate_tar(self, path, comp, remaining):
        if comp:
            mode = f"r:{comp}"
        else:
            mode = "r"
        with tarfile.open(path, mode) as th:
            if self.increment_total and len(remaining) == 1:
                names = th.namelist()
                self.update_progress_bar(increment_total=len(names))
                del names
            ti = th.next()
            while ti is not None:
                if ti.isfile():
                    yield TarPointer(th, ti)
                ti = th.next()

    def file_opener(self, path, comp):
        if not comp:
            if isinstance(path, io.IOBase):
                # already a file handle
                return path
            elif isinstance(path, Pointer):
                return path.get_handle()
        elif isinstance(path, Pointer):
            path = path.get_handle()
        elif isinstance(path, dict):
            # URGH
            self.load_manager.log(logging.ERROR, f"[red]Got a dict as path in file_opener for {self.name}: {path}")
            return None

        if comp == 'gz':
            return gzip.open(path)
        elif comp == 'bz2':
            return bz2.open(path)
        elif not comp:
            try:
                return open(path)
            except:
                self.load_manager.log(logging.ERROR, f"[red]Got something we couldn't open for {self.name}: {path}")
                return None
        else:
            # Dunno what this is
            return None

    def count_lines(self, fh):
        # Simple method of just read in the lines
        # mmap doesn't work on compressed
        # And have already opened the file, so no point using binary read
        # Could read in chunks for some time saving

        try:
            length = fh.seek(0, os.SEEK_END)
            fh.seek(0)
        except:
            # No seek? :(
            return -1
        lines = 0
        if length < 100000000:
            for l in fh:
                lines += 1
            fh.seek(0)
            return lines
        else:
            return -1

    def iterate_lines(self, path, comp, remaining):
        with self.file_opener(path, comp) as fh:

            if self.increment_total:
                lines = self.count_lines(fh)
                if lines > 0:
                    self.update_progress_bar(increment_total=lines)

            l = fh.readline()
            while l:
                if type(l) == str:
                    yield io.StringIO(l)
                elif type(l) == bytes:
                    yield io.BytesIO(l)
                l = fh.readline()

    def iterate_dict(self, path, comp, remaining):
        with self.file_opener(path, comp) as fh:
            data = json.load(fh)
            if self.increment_total and len(remaining) == 1:
                self.update_progress_bar(increment_total=len(data))
            for v in data.values():
                yield v

    def iterate_array(self, path, comp, remaining):
        with self.file_opener(path, comp) as fh:
            data = json.load(fh)
            if self.increment_total and len(remaining) == 1:
                self.update_progress_bar(increment_total=len(data))
            # This is yield actual json, not a file/string of json
            for v in data:
                yield v

    def iterate_arraylines(self, path, comp, remaining):
        # Assumptions:  array of json, where each record is a line
        with self.file_opener(path, comp) as fh:

            if self.increment_total:
                lines = self.count_lines(fh)
                if lines > 0:
                    self.update_progress_bar(increment_total=lines)

            # And this is the same hard case
            l = True
            while l:
                l = fh.readline()
                if not l:
                    break
                if type(l) == bytes:
                    l = l.decode("utf-8")
                l = l.strip()
                if l[0] in ['[', ',']:
                    l = l[1:]
                elif l[-1] in [']', ',']:
                    l = l[:-1]
                l = l.strip()
                if len(l) < 2:
                    continue
                elif l[0] != '{' or l[-1] != '}':
                    continue
                yield io.StringIO(l)

    def make_raw(self, path, comp, parent):
        # path is the actual native JSON record
        data = self.post_process_json(path)
        ident = self.extract_identifier(data)
        if not ident:
            raise ValueError(f"Could not get an identifier in {self.name} while in {parent}")
        return {'identifier': ident, 'data': data}

    def make_json(self, path, comp, parent):
        ident = self.make_identifier(path)
        with self.file_opener(path, comp) as fh:
            data = json.load(fh)
        data = self.post_process_json(data)
        if not ident:
            ident = self.extract_identifier(data)
            if not ident:
                raise ValueError(f"Could not get an identifier in {self.name} while in {parent}/{path}")
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
                raise ValueError(f"Could not get an identifier in {self.name} while in {parent}/{path}")
        return {'identifier': ident, 'data': data}

    def make_identifier(self, value):
        # assume a filepath with the last component as the identifier
        if isinstance(value, Pointer):
            value = value.get_name()
        elif hasattr(value, 'name'):
            value = value.name
        elif isinstance(value, bytes):
            value = value.decode('utf-8')
        try:
            last = value.split('/')[-1]
            return last.split('.')[0]
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
        if not self.overwrite and data['identifier'] in self.out_cache:
            return False
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
            lblfn = os.path.join(self.configs.temp_dir, f"{self.name}_labels_{self.my_slice}.tsv")
            lbl = open(lblfn, 'w')
            self.temp_file_handles['label'] = lbl
        if 'inverseEquivDbPath' in self.config:
            eqfn = os.path.join(self.configs.temp_dir, f"{self.name}_equivs_{self.my_slice}.tsv")
            eq = open(eqfn, 'w')
            self.temp_file_handles['equiv'] = eq
        if 'hasDifferentFrom' in self.config:
            diffn = os.path.join(self.configs_temp_dir, f"{self.name}_diffs_{self.my_slice}.tsv")
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
        except Exception as e:
            logger.error(e)
            return False
        self.increment_progress_bar(1)
        return True

    def process_step(self, steps, path, parent):
        step = steps[0]
        comp = step[1] if len(step) > 1 else None
        handler = self.step_functions[step[0]]
        if step[0] in self.fmt_containers:
            for child in handler(path, comp, steps[1:]):
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
            raise ValueError(f"Unknown step type {step} in {self.name}")

    def update_progress_bar(self, total=-1, increment_total=-1):
        if total > 0:
            ttl = total
            self.total = ttl
        elif increment_total > 0:
            self.total += increment_total
            ttl = self.total
        else:
            ttl = self.total
        if self.max_slice > 1:
            ttl = ttl // self.max_slice
        desc = f"{self.name}/{self.my_slice}"
        self.load_manager.update_progress_bar(total=ttl, description=desc)

    def increment_progress_bar(self, amount):
        self.load_manager.update_progress_bar(advance=1)

    def close_progress_bar(self):
        # Could set visibility to false or something but better to just leave it
        pass

    def prepare_load(self, mgr, my_slice=0, max_slice=0, load_type="records"):
        self.load_manager = mgr
        self.my_slice = my_slice
        self.max_slice = max_slice

        if load_type == "export":
            # Unknown calculate from file
            self.total = -1

        files = []
        if (ifs := self.config.get('input_files', {})):
            if not load_type in ifs:
                self.log(logging.WARNING, "[red]No configured file for load type {load_type} in {self.name}")
            for p in ifs[load_type]:
                fmt = p.get('type', None)
                path = p.get('path', None)
                url = p.get('url', None)
                if url is None and path is None:
                    # WTF?
                    continue
                elif path is None:
                    path = url.split('/')[-1]
                if fmt:
                    fmtspec = self.process_fmt(fmt)
                else:
                    # gotta guess
                    fmtspec = self.guess_fmt(path)

                if not '/' in path:
                    path = os.path.join(self.dumps_dir, path)
                files.append({"path": path, "fmt": fmtspec})

        if not files and load_type == "records" and (dfp := self.config.get('dumpFilePath')):
            # look in dfp
            fmt = self.config.get('dumpFileType', None)
            if fmt:
                fmtspec = self.process_fmt(fmt)
            else:
                # Guessing again
                fmtspec = self.guess_fmt(dfp)
            files.append({"path": dfp, "fmt": fmtspec})
        self.my_files = files


    def load(self, disable_ui=False, verbose=False, overwrite=True):
        self.overwrite = overwrite
        self.increment_total = self.total < 0

        self.open_temp_files()
        for info in self.my_files:
            if not disable_ui:
                self.update_progress_bar()
            self.load_manager.log(logging.INFO, f"[green]Loading {info['path']} for {self.name} in {self.my_slice}")
            try:
                self.process_step(info['fmt'], info['path'], None)
            except Exception as e:
                self.load_manager.log(logging.ERROR, f"[red]Failed to load file:")
                self.load_manager.log(logging.ERROR, e)
            if not disable_ui:
                self.close_progress_bar()
        try:
            self.out_cache.commit()
        except:
            pass
        self.close_temp_files()
