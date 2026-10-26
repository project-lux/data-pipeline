"""Pleiades publishes two files, both named in the source's ``input_files``
(``records``) block and fetched by the base class into ``dumps_dir``:

    Types url:  https://atlantides.org/downloads/pleiades/rdf/place-types.ttl
    Places url: https://atlantides.org/downloads/pleiades/json/pleiades-places-latest.json.gz

The class is kept so configurations may name it; it adds nothing to
`BaseDownloader.get_urls`, which already turns every ``input_files`` entry
with a ``url`` into a download under ``dumps_dir`` and honours the
manager's ``type``.
"""

from lux_pipeline.process.base.downloader import BaseDownloader


class PleiadesDownloader(BaseDownloader):
    pass
