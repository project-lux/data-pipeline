#!/usr/bin/env python3
"""Crawl YPM agent JSON files from the Apache directory listing.

Walks https://images.peabody.yale.edu/data/agent/ -> hex dirs (0-f) ->
two-character subdirs, parses each listing for .json entries, and downloads
every file whose listed size is non-zero. Files are written mirroring the
remote directory structure; existing non-empty local files are skipped, so
the script can be re-run to resume.
"""

import argparse
import re
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import requests

BASE_URL = "https://images.peabody.yale.edu/data/object/"

# Matches one file row in the Apache fancy-index table: the href and the
# Size cell (last right-aligned <td>), e.g. '  0 ', '1.0K', '702 '
ROW_RE = re.compile(
    r'<a href="(?P<href>[^"?/][^"]*)">.*?'
    r'<td align="right">\s*(?P<size>[\d.]+[KMG]?)\s*</td>\s*'
    r"<td>&nbsp;</td>",
    re.DOTALL,
)
DIR_RE = re.compile(r'<a href="(?P<href>[0-9a-f]{1,2}/)">')


def fetch(session, url, retries=3):
    for attempt in range(retries):
        try:
            resp = session.get(url, timeout=60)
            resp.raise_for_status()
            return resp
        except requests.RequestException as e:
            if attempt == retries - 1:
                raise
            print(f"  retry {attempt + 1} for {url}: {e}", file=sys.stderr)
    return None


def list_subdirs(session, url):
    html = fetch(session, url).text
    return [url + m.group("href") for m in DIR_RE.finditer(html)]


def list_json_files(session, url):
    """Return [(filename, size_string)] for .json entries in a listing."""
    html = fetch(session, url).text
    return [
        (m.group("href"), m.group("size"))
        for m in ROW_RE.finditer(html)
        if m.group("href").endswith(".json")
    ]


def download_dir(session, dir_url, out_root):
    """Process one leaf directory; returns (downloaded, skipped_empty, skipped_existing)."""
    rel = dir_url[len(BASE_URL) :]  # e.g. "0/0a/"
    out_dir = out_root / rel
    out_dir.mkdir(parents=True, exist_ok=True)

    downloaded = empty = existing = 0
    for fn, size in list_json_files(session, dir_url):
        if size == "0":
            empty += 1
            continue
        target = out_dir / fn
        if target.exists() and target.stat().st_size > 0:
            existing += 1
            continue
        resp = fetch(session, dir_url + fn)
        target.write_bytes(resp.content)
        downloaded += 1
    return downloaded, empty, existing


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--out", default="ypm_agents", help="output directory")
    ap.add_argument(
        "--workers", type=int, default=8, help="concurrent leaf-directory workers"
    )
    args = ap.parse_args()

    out_root = Path(args.out)
    session = requests.Session()
    adapter = requests.adapters.HTTPAdapter(pool_maxsize=args.workers * 2)
    session.mount("https://", adapter)

    print(f"Listing top-level directories under {BASE_URL}")
    top_dirs = list_subdirs(session, BASE_URL)
    print(f"Found {len(top_dirs)} top-level directories; listing subdirectories...")

    leaf_dirs = []
    for d in top_dirs:
        leaf_dirs.append(list_subdirs(session, d))
        print(f"  {d} -> {len(leaf_dirs[-1])} subdirectories")
    leaf_dirs = [u for subs in leaf_dirs for u in subs]
    print(f"{len(leaf_dirs)} leaf directories to process")

    totals = [0, 0, 0]
    errors = []
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {
            pool.submit(download_dir, session, u, out_root): u for u in leaf_dirs
        }
        for n, fut in enumerate(as_completed(futures), 1):
            url = futures[fut]
            try:
                result = fut.result()
            except Exception as e:
                errors.append(url)
                print(f"[{n}/{len(leaf_dirs)}] FAILED {url}: {e}", file=sys.stderr)
                continue
            for i, v in enumerate(result):
                totals[i] += v
            if n % 50 == 0 or n == len(leaf_dirs):
                print(
                    f"[{n}/{len(leaf_dirs)}] downloaded={totals[0]} "
                    f"empty-skipped={totals[1]} already-present={totals[2]}"
                )

    print(
        f"Done. Downloaded {totals[0]} files; skipped {totals[1]} zero-size "
        f"and {totals[2]} already present."
    )
    if errors:
        print(f"{len(errors)} directories failed:", file=sys.stderr)
        for url in errors:
            print(f"  {url}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
