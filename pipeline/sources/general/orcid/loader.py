
from pipeline.process.base.loader import Loader
import glob
import gzip
import os
import re
import tarfile
import time
import ujson as json

# The 2025 dump has ~26.3M members. Only used for the progress ETA, and only
# when the source config doesn't give a totalRecords.
TOTAL_MEMBERS = 26300000

# Measured on the 2025 dump: level 3 compresses at 329MB/s into 84GB of
# shards, against 351MB/s / 104GB at level 1 and 123MB/s / 65GB at level 6.
# Compression is the expensive half of split(), so this wants to stay off the
# steep end of that curve.
GZIP_LEVEL = 3

#
# An empty record looks like this:
# <person:person path="/0009-0009-9733-1001/person">
#     <person:name visibility="public" path="0009-0009-9733-1001">
#         <common:created-date>2023-04-21T05:28:48.830Z</common:created-date>
#         <common:last-modified-date>2023-04-21T05:28:48.830Z</common:last-modified-date>
#         <personal-details:given-names>Shyamala</personal-details:given-names>
#         <personal-details:family-name>A</personal-details:family-name>
#     </person:name>
#     <other-name:other-names path="/0009-0009-9733-1001/other-names"/>
#     <researcher-url:researcher-urls path="/0009-0009-9733-1001/researcher-urls"/>
#     <email:emails path="/0009-0009-9733-1001/email"/>
#     <address:addresses path="/0009-0009-9733-1001/address"/>
#     <keyword:keywords path="/0009-0009-9733-1001/keywords"/>
#     <external-identifier:external-identifiers path="/0009-0009-9733-1001/external-identifiers"/>
# </person:person>
# <activities:activities-summary path="/0009-0009-9733-1001/activities">
#     <activities:distinctions path="/0009-0009-9733-1001/distinctions"/>
#     <activities:educations path="/0009-0009-9733-1001/educations"/>
#     <activities:employments path="/0009-0009-9733-1001/employments"/>
#     <activities:fundings path="/0009-0009-9733-1001/fundings"/>
#     <activities:invited-positions path="/0009-0009-9733-1001/invited-positions"/>
#     <activities:memberships path="/0009-0009-9733-1001/memberships"/>
#     <activities:peer-reviews path="/0009-0009-9733-1001/peer-reviews"/>
#     <activities:qualifications path="/0009-0009-9733-1001/qualifications"/>
#     <activities:research-resources path="/0009-0009-9733-1001/research-resources"/>
#     <activities:services path="/0009-0009-9733-1001/services"/>
#     <activities:works path="/0009-0009-9733-1001/works"/>
# </activities:activities-summary>


class OrcidLoader(Loader):

    def should_load(self, data):
        # Only care if there are one or more fields beyond name and orcid
        # And then only things we really care about, which is -not- works

        # Biography
        if data.find(b'<person:biography') != -1: return True

        # Identifiers
        if data.find(b'/researcher-urls"/>') == -1: return True
        if data.find(b'/email"/>') == -1: return True
        # if data.find(b'/address"/>') == -1: return True
        if data.find(b'/keywords"/>') == -1: return True
        if data.find(b'/external-identifiers"/>') == -1: return True

        # Links
        if data.find(b'/employments"/>') == -1: return True
        if data.find(b'/fundings"/>') == -1: return True
        if data.find(b'/memberships"/>') == -1: return True

        # Otherwise we don't really care?
        return False

    
    def _shard_base(self):
        base = self.in_path
        for ext in (".tgz", ".tar.gz"):
            if base.endswith(ext):
                base = base[: -len(ext)]
                break
        return base

    def shard_path(self, n):
        # Shards sit beside the dump they were split from and are named after
        # it, so next year's dump can't quietly load this year's records --
        # they simply aren't found under its own name.
        return f"{self._shard_base()}_{n:03d}.jsonl.gz"

    def find_shards(self):
        # Shards are written to a .tmp name and renamed once complete, so
        # anything visible here is a whole shard.
        found = {}
        for fn in glob.glob(f"{self._shard_base()}_*.jsonl.gz"):
            m = re.search(r"_(\d+)\.jsonl\.gz$", fn)
            if m:
                found[int(m.group(1))] = fn
        if not found:
            return []
        missing = [n for n in range(max(found) + 1) if n not in found]
        if missing:
            # Loading the rest would look like it worked and silently drop
            # every record those slices held
            raise ValueError(
                f"orcid shards {missing} are missing from {self._shard_base()}_*.jsonl.gz; "
                f"re-run split-orcid.py for those slices, or delete the shards to "
                f"load from {self.in_path} instead"
            )
        return [found[n] for n in range(max(found) + 1)]

    def load(self, slicen=None, maxSlice=None):
        # Committing inside every set() costs an fsync per record. Batch them
        # instead: checkpoint() below marks the boundary a commit is allowed to
        # land on -- one archive member, fully processed -- and the cache does
        # the counting. Deferral expects parallel workers to write disjoint
        # keys, which holds here: slices take disjoint members of the archive,
        # and each member is one ORCID id.
        self.out_cache.defer_commits(every=1000)
        try:
            shards = self.find_shards()
            if shards:
                self._load_shards(shards, slicen, maxSlice)
            else:
                self._load_dump(slicen, maxSlice)
        finally:
            # Deferral is process-wide -- every cache in the process shares one
            # write connection -- so hand it back however we leave. This lands
            # the last partial batch, so it replaces the old commit() and runs
            # on the way out of an exception too.
            self.out_cache.resume_commits()

    def _load_shards(self, shards, slicen, maxSlice):
        # A shard holds the records one split slice kept, already filtered and
        # parsed out of the archive. The number of shards and the number of
        # loader workers don't have to match: a worker takes every shard whose
        # index falls in its own slice.
        if maxSlice is None:
            mine = list(enumerate(shards))
        else:
            mine = [(n, fn) for n, fn in enumerate(shards) if n % maxSlice - slicen == 0]
        if not mine:
            print(f"No shards for slice {slicen} of {maxSlice}: only {len(shards)} shards exist")
            return
        print(f"Loading {len(mine)} of {len(shards)} orcid shards: {[n for n, fn in mine]}")

        x = 0
        start = time.time()
        for n, fn in mine:
            with gzip.open(fn, "rt") as fh:
                for line in fh:
                    ident, jstr = line.strip().split("\t", 1)
                    self.out_cache[ident] = json.loads(jstr)
                    # Record is done: the cache may commit here, and only here
                    self.out_cache.checkpoint()
                    x += 1
                    if not x % 10000:
                        t = time.time() - start
                        print(f"{x} in {t} = {x / t}/s (shard {n})")

    def split(self, slicen=None, maxSlice=None):
        """Write the archive out as JSONL shards, ready to load.

        The archive is one gzip stream, so it can't be seeked into: every
        loader worker has to walk all 46GB to find its own slice, and 56% of
        what it decompresses is then dropped by should_load(). Doing that walk
        once per environment is the cost this removes -- afterwards each worker
        reads only the records it will store, out of a file of its own.

        With a slice, this writes that one shard, so the walk can be run in
        parallel; without one it writes all maxSlice shards in a single pass.
        Either way shard n holds the same records, so the two can be mixed.

        The filter is baked into the shards, so re-run this if should_load()
        changes."""
        if maxSlice is None:
            maxSlice = 1
        mine = [slicen] if slicen is not None else list(range(maxSlice))

        # Shards are written under a .tmp name and renamed once the walk
        # finishes, so a split that dies partway leaves nothing that
        # find_shards() will mistake for a complete shard.
        paths = {n: self.shard_path(n) for n in mine}
        handles = {n: gzip.open(f"{paths[n]}.tmp", "wt", GZIP_LEVEL) for n in mine}
        kept = {n: 0 for n in mine}
        print(f"Writing {len(mine)} of {maxSlice} shards: {', '.join(paths[n] for n in mine)}")

        ttl = self.total if self.total > 0 else TOTAL_MEMBERS
        tf = tarfile.open(self.in_path, "r:gz")
        nxt = tf.next()
        x = 0
        mine_x = 0
        done_x = 0
        start = time.time()
        try:
            while nxt is not None:
                if not nxt.name.endswith('xml'):
                    nxt = tf.next()
                    continue
                # Which shard a record belongs to is decided the same way the
                # loader used to pick its slice out of the archive, so shard n
                # holds exactly what worker n would have loaded from the .tgz
                fh = handles.get(x % maxSlice)
                if fh is None:
                    x += 1
                    nxt = tf.next()
                    continue
                mine_x += 1
                ident = nxt.name.rsplit('/', 1)[-1].replace('.xml', '')
                rech = tf.extractfile(nxt)
                data = rech.read()
                rech.close()
                if self.should_load(data):
                    done_x += 1
                    kept[x % maxSlice] += 1
                    fh.write(f"{ident}\t{json.dumps({'xml': data.decode('utf-8')})}\n")
                nxt = tf.next()
                x += 1
                if not x % 100000:
                    t = time.time() - start
                    xps = x / t
                    ttls = ttl / xps
                    print(f"{x} in {t} = {xps}/s --> {ttls} total ({ttls/3600} hrs)")
                    print(f"{done_x} with data / {mine_x} = {done_x/mine_x*100}%")
        finally:
            for fh in handles.values():
                fh.close()
            tf.close()

        for n, path in paths.items():
            os.rename(f"{path}.tmp", path)
            print(f"Wrote {kept[n]} records to {path} "
                  f"({os.path.getsize(path)/1e9:.1f}GB)")
        print(f"{done_x} records from {mine_x} members in {time.time() - start}s")

    def _load_dump(self, slicen, maxSlice):
        # This loads from annual summary dump file.
        # It strips records with only a name
        # dump file is a single tar.gz of files

        tf = tarfile.open(self.in_path, "r:gz")
        nxt = tf.next()
        x = 0
        mine_x = 0
        done_x = 0
        start = time.time()
        ttl = self.total if self.total > 0 else TOTAL_MEMBERS
        while nxt is not None:
            if not nxt.name.endswith('xml'):
                nxt = tf.next()
                continue
            # Slice by member: a tar.gz is a single gzip stream, so every
            # worker has to walk the whole archive, but only its own slice is
            # extracted, decoded, filtered and written -- which is where the
            # time goes. Members come out in archive order, so the slices are
            # the same in every worker and never overlap.
            if maxSlice is not None and x % maxSlice - slicen != 0:
                x += 1
                nxt = tf.next()
                continue
            mine_x += 1
            ident = nxt.name.rsplit('/', 1)[-1].replace('.xml', '')
            rech = tf.extractfile(nxt)
            data = rech.read()
            rech.close()
            if self.should_load(data):
                done_x += 1
                self.out_cache[ident] = {"xml": data.decode('utf-8')}
                # Record is done: the cache may commit here, and only here
                self.out_cache.checkpoint()
            nxt = tf.next()
            x+=1
            if not x % 100000:
                t = time.time() - start
                xps = x/t
                ttls = ttl / xps
                print(f"{x} in {t} = {xps}/s --> {ttls} total ({ttls/3600} hrs)")
                # done_x is out of the members this worker owns, not the ones
                # it walked past
                print(f"{done_x} with data / {mine_x} = {done_x/mine_x*100}%")
        tf.close()
