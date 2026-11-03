"""Base downloader: turn a source's configuration into the files it needs
and fetch them.

A downloader answers two questions for the download manager (item 15):
*which* files does this source need (`get_urls`), and *how* is one of them
fetched (`process`). Sources whose dump lives behind a "splash page" (a
Zenodo record, a GitHub release, a page with a "latest" link) subclass this
and override `get_urls`, using `get_value_from_json` and friends to discover
the real URL.

Configuration keys, all per source and all optional:

``remoteDumpFile`` + ``dumpFilePath``
    One URL and the file it is saved to. Both must be present for the pair
    to count. ``dumpFilePath`` is a path type (``path_types`` in the base
    config) so the `Config` has usually already made it absolute.

``input_files``
    The same structure the loader (item 12) reads::

        "input_files": {
            "records": [{"url": "https://…/data.zip", "path": "data.zip",
                         "type": "zip/json"}],
            "export":  [{"url": "https://…/export.tgz"}]
        }

    Each entry with a ``url`` is downloaded to its ``path``; an entry with
    no ``path`` is saved under the file name at the end of the URL. Entries
    without a ``url`` describe local files and are skipped here.

``dumps_dir``
    A sub-directory of the base configuration's ``dumps_dir`` where this
    source's files live. Relative paths resolve against it, exactly as the
    loader resolves them, so the two always agree on where a file is.

``download_verify_ssl``
    Verify TLS certificates (default ``True``). Set to ``false`` only for a
    host whose certificate chain is known to be broken; the same key on the
    base configuration applies to every source.

A file is streamed to ``<path>.part`` and renamed into place when the
transfer completes, so an interrupted download never leaves a truncated
file for the loader to find. A download entry may carry a ``checksum``
(``"md5:<hex>"`` or ``"sha256:<hex>"``; a bare hex is MD5), which is
verified over the stream before the rename. Every request sends a
descriptive ``User-Agent`` (`USER_AGENT`): some hosts, Zenodo among
them, may refuse the default library one.
"""

import hashlib
import json
import logging
import os
import re
from pathlib import Path

import requests

from lux_pipeline.progress import Sliced

logger = logging.getLogger("lux_pipeline")

_INDEX_RE = re.compile(r"\[(\d+)\]")

USER_AGENT = "lux-pipeline/0.1 (+https://github.com/project-lux/data-pipeline)"


class BaseDownloader(Sliced):
    """Provide URLs to the download manager and fetch them one at a time.

    `prepare(my_slice, max_slice, progress)` comes from `Sliced`; the
    manager hands each worker whole files, so the slice is informational.
    """

    chunk_size = 64 * 1024
    timeout = (10, 60)
    user_agent = USER_AGENT

    def __init__(self, config):
        self.config = config
        self.name = config.get("name", "")
        self.configs = config.get("all_configs")
        self.dumps_dir = getattr(self.configs, "dumps_dir", "") or ""
        if config.get("dumps_dir"):
            self.dumps_dir = os.path.join(self.dumps_dir, config["dumps_dir"])

    # --- discovering URLs -------------------------------------------------

    def verify_ssl(self):
        """Whether TLS certificates are checked for this source."""
        if "download_verify_ssl" in self.config:
            return bool(self.config["download_verify_ssl"])
        return bool(getattr(self.configs, "download_verify_ssl", True))

    def resolve_path(self, path):
        """Make a configured file path absolute, relative to ``dumps_dir``."""
        if os.path.isabs(path):
            return path
        if not self.dumps_dir:
            raise ValueError(f"{self.name}: no dumps_dir to resolve download path {path!r} against")
        return os.path.join(self.dumps_dir, path)

    def get_urls(self, type="all"):
        """Return ``[{"url": …, "path": …}, …]`` for the files this source needs.

        ``type`` is ``all`` or a key of ``input_files`` (``records``,
        ``export``, …). The ``remoteDumpFile`` pair counts as ``records``.
        A source that configures no downloads returns an empty list.
        """
        urls = []
        url = self.config.get("remoteDumpFile")
        path = self.config.get("dumpFilePath")
        if url and path and type in ("all", "records"):
            urls.append({"url": url, "path": self.resolve_path(path)})

        for rtype, entries in (self.config.get("input_files") or {}).items():
            if type not in ("all", rtype):
                continue
            for entry in entries or []:
                url = entry.get("url")
                if not url:
                    logger.debug(f"{self.name}: input file {entry.get('path')!r} has no url; nothing to download")
                    continue
                path = entry.get("path") or url.rstrip("/").rsplit("/", 1)[-1]
                if not path:
                    raise ValueError(f"{self.name}: cannot derive a file name for {url}")
                urls.append({"url": url, "path": self.resolve_path(path)})
        return urls

    # --- splash-page helpers ---------------------------------------------

    def headers(self):
        return {"User-Agent": self.user_agent}

    def fetch_webpage(self, url):
        """Return the text at ``url``, or ``None`` (logged) if the request fails."""
        try:
            response = requests.get(url, timeout=self.timeout, verify=self.verify_ssl(),
                                    headers=self.headers())
            response.raise_for_status()
            return response.text
        except requests.RequestException as e:
            logger.error(f"{self.name}: error fetching {url}: {e}")
            return None

    def fetch_json(self, url):
        """The parsed JSON at ``url`` (redirects followed), or ``None``
        (logged) when the request fails; a non-JSON or empty body raises."""
        response = self.fetch_webpage(url)
        if response is None:
            return None
        try:
            data = json.loads(response)
        except ValueError as e:
            raise ValueError(f"{self.name}: failed to parse JSON from {url}: {e}")
        if not data:
            raise ValueError(f"{self.name}: received empty JSON response from {url}")
        return data

    def json_value(self, data, data_key, base_url=""):
        """The string at ``data_key`` in parsed ``data``, else ``None``
        (logged). ``data_key`` is a dotted path with optional list indexes,
        e.g. ``hits.hits[0].files[0].links.self``; slashes are accepted in
        place of dots."""
        if "/" in data_key and "." not in data_key:
            data_key = data_key.replace("/", ".")
        try:
            value = self._walk_json(data, data_key)
        except (IndexError, KeyError, TypeError) as e:
            logger.error(f"{self.name}: cannot follow JSON path {data_key!r} in {base_url}: {e!r}")
            return None
        if not isinstance(value, str):
            logger.error(f"{self.name}: JSON path {data_key!r} in {base_url} is not a string: {value!r}")
            return None
        return value

    def get_value_from_json(self, base_url, data_key):
        """Fetch JSON from ``base_url`` and return the string at ``data_key``
        (see `json_value`), or ``None`` (logged)."""
        data = self.fetch_json(base_url)
        if data is None:
            return None
        return self.json_value(data, data_key, base_url)

    @staticmethod
    def _walk_json(data, data_key):
        value = data
        for key in data_key.split("."):
            if not key:
                continue
            name = _INDEX_RE.split(key)[0]
            if name:
                value = value[name]
            for index in _INDEX_RE.findall(key):
                value = value[int(index)]
        return value

    def get_value_from_html(self, base_url, xpath):
        raise NotImplementedError("HTML splash pages are not supported yet; override get_urls")

    def get_value_from_xml(self, base_url, xpath):
        raise NotImplementedError("XML splash pages are not supported yet; override get_urls")

    # --- fetching ----------------------------------------------------------

    @staticmethod
    def parse_checksum(value):
        """``(algorithm, hex)`` from ``"md5:abc..."``, ``"sha256:..."`` or a
        bare hex (MD5); ``None`` for an empty or unknown value."""
        if not value or not isinstance(value, str):
            return None
        algo, _, digest = value.partition(":")
        if not digest:
            algo, digest = "md5", algo
        algo = algo.lower()
        if algo not in hashlib.algorithms_available:
            logger.warning(f"Unknown checksum algorithm {algo!r}; not verifying")
            return None
        return algo, digest.strip().lower()

    def process(self, download, overwrite=True):
        """Stream ``download["url"]`` to ``download["path"]``.

        Returns 1 when a file was written and 0 when an existing file was
        kept because ``overwrite`` is false. Raises on any HTTP or transfer
        error, or a ``checksum`` mismatch, after removing the partial file.
        """
        url = download["url"]
        path = download["path"]
        filename = os.path.basename(path)
        label = f"{self.name}/{filename}" if self.name else filename
        checksum = self.parse_checksum(download.get("checksum"))
        hasher = hashlib.new(checksum[0]) if checksum else None

        if not overwrite and os.path.exists(path):
            logger.info(f"{label}: exists, not overwriting")
            self.progress(description=f"{label} (kept)")
            return 0

        Path(path).parent.mkdir(parents=True, exist_ok=True)
        part = path + ".part"
        written = 0
        try:
            with requests.get(url, stream=True, timeout=self.timeout, verify=self.verify_ssl(),
                              headers=self.headers()) as response:
                response.raise_for_status()
                total = int(response.headers.get("content-length") or 0)
                if total:
                    self.progress(total=total, completed=0, description=label)
                else:
                    self.progress(description=label)
                with open(part, "wb") as fh:
                    for chunk in response.iter_content(chunk_size=self.chunk_size):
                        if chunk:
                            size = fh.write(chunk)
                            written += size
                            if hasher is not None:
                                hasher.update(chunk)
                            self.progress(advance=size)
            if total and written != total:
                raise IOError(f"{label}: short download, {written:,} of {total:,} bytes")
            if hasher is not None and hasher.hexdigest() != checksum[1]:
                raise IOError(f"{label}: {checksum[0]} checksum mismatch, got {hasher.hexdigest()} "
                              f"expected {checksum[1]}")
        except BaseException:
            try:
                os.remove(part)
            except FileNotFoundError:
                pass
            raise
        os.replace(part, path)
        self.progress(completed=written)
        logger.info(f"Finished downloading {label} ({written:,} bytes)")
        return 1
