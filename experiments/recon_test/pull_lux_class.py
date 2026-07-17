"""Pull one LUX class from the cluster store into a local LMDB.

Same wire protocol and local layout as lux-concepts-scc (sub-db "data",
key = 16-byte UUID, value = zlib(JSON) verbatim from the remote store),
so concepts tooling and legacy_compare.py work on any class unchanged.

  python pull_lux_class.py --cls Person --out /Users/wjm55/lux-data/person.lmdb
"""

import argparse
import shlex
import struct
import subprocess
import sys
import time

import lmdb

REMOTE_HOST = "yale-cluster"
REMOTE_DB = "/nfs/roberts/pi/pi_rs2668/lux/lux_store.lmdb"
REMOTE_UV = "/home/wjm55/.local/bin/uv"

REMOTE_SCRIPT = r"""
import struct, sys, lmdb
DB_PATH = sys.argv[1]
CLS     = sys.argv[2].encode("utf-8")
env = lmdb.open(DB_PATH, max_dbs=3, readonly=True, lock=False, readahead=False)
data = env.open_db(b"data", dupsort=False)
idx  = env.open_db(b"index", dupsort=True)
out = sys.stdout.buffer
total = 0
with env.begin() as txn:
    c = txn.cursor(db=idx)
    if c.set_range(CLS) and bytes(c.key()) == CLS:
        for v in c.iternext_dup():
            total += len(v) // 16
out.write(b"__count__ %d\n" % total); out.flush()
with env.begin(buffers=True) as txn:
    c = txn.cursor(db=idx)
    if c.set_range(CLS) and bytes(c.key()) == CLS:
        for packed in c.iternext_dup():
            for i in range(0, len(packed), 16):
                bk = bytes(packed[i:i+16])
                val = txn.get(key=bk, db=data)
                if val is None:
                    continue
                vb = bytes(val)
                out.write(bk)
                out.write(struct.pack(">I", len(vb)))
                out.write(vb)
out.flush()
"""


def read_exact(stream, n):
    buf = bytearray()
    while len(buf) < n:
        chunk = stream.read(n - len(buf))
        if not chunk:
            return bytes(buf)
        buf.extend(chunk)
    return bytes(buf)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--cls", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--host", default=REMOTE_HOST)
    ap.add_argument("--db", default=REMOTE_DB)
    ap.add_argument("--map-size", type=int, default=64 << 30)
    args = ap.parse_args()

    remote_cmd = (f"{shlex.quote(REMOTE_UV)} run --with lmdb --python 3.12 "
                  f"python3 - {shlex.quote(args.db)} {shlex.quote(args.cls)}")
    proc = subprocess.Popen(
        ["ssh", "-o", "BatchMode=yes", "-o", "ServerAliveInterval=30",
         args.host, remote_cmd],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL, bufsize=0)
    proc.stdin.write(REMOTE_SCRIPT.encode())
    proc.stdin.close()

    header = proc.stdout.readline()
    if not header.startswith(b"__count__ "):
        print(f"bad header: {header!r}", file=sys.stderr)
        sys.exit(1)
    total = int(header.split()[1])
    print(f"{args.cls}: {total} records", flush=True)

    env = lmdb.open(args.out, max_dbs=2, map_size=args.map_size, subdir=True)
    db = env.open_db(b"data")
    txn = env.begin(write=True, db=db)
    n = 0
    nbytes = 0
    start = time.time()
    while True:
        bk = read_exact(proc.stdout, 16)
        if not bk:
            break
        if len(bk) < 16:
            print("truncated stream (uuid)", file=sys.stderr)
            sys.exit(1)
        (length,) = struct.unpack(">I", read_exact(proc.stdout, 4))
        payload = read_exact(proc.stdout, length)
        if len(payload) < length:
            print("truncated stream (payload)", file=sys.stderr)
            sys.exit(1)
        txn.put(bk, payload, db=db)
        n += 1
        nbytes += length
        if n % 5000 == 0:
            txn.commit()
            txn = env.begin(write=True, db=db)
        if n % 250000 == 0:
            rate = n / (time.time() - start)
            print(f"  {n}/{total} ({rate:.0f}/s, {nbytes/1e9:.2f} GB)",
                  flush=True)
    txn.commit()
    env.close()
    rc = proc.wait()
    print(f"done: {n}/{total} records, {nbytes/1e9:.2f} GB, "
          f"{time.time()-start:.0f}s, ssh rc={rc}", flush=True)
    if n != total or rc != 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
