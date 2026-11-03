"""BnF: tar.gz files of RDF/XML, each file holding ``rdf:Description``
nodes for many entities (``.../ark:/12148/cb...`` and ``...#about``).
The mapper wants, per entity, an ``rdf:RDF`` document holding every
node of that entity, the shape the fetcher stores (``{"xml": ...}``), so
the ``entities`` container groups the nodes of each file by their ARK id
and yields one such document per entity; the ``raw`` format then stores
it under the ARK id (``dumpFileType: tar.gz/entities/raw``). The dumps
say ``http://data.bnf.fr``; the mapper's XPaths and the live site say
``https``, so the documents are normalised to ``https``.
"""

import logging
import re

from lxml import etree

from lux_pipeline.process.base.loader import Loader

logger = logging.getLogger("lux_pipeline")

RDF_ABOUT = "{http://www.w3.org/1999/02/22-rdf-syntax-ns#}about"
ARK = re.compile(r"^https?://data\.bnf\.fr/ark:/12148/([^#/]+)")
ABOUT_RE = re.compile(r'rdf:about="https?://data\.bnf\.fr/ark:/12148/([^"#/]+)')


class BnfLoader(Loader):
    extra_containers = ("entities",)

    def _init_dsl(self):
        super()._init_dsl()
        self.step_functions["entities"] = self.iterate_entities

    def iterate_entities(self, path, comp, remaining):
        """One ``{"xml": <rdf:RDF ...>}`` per entity in the file, holding
        every description whose ``rdf:about`` is that entity's ARK."""
        with self.file_opener(path, comp) as fh:
            data = fh.read()
        try:
            dom = etree.XML(data)
        except Exception as e:
            logger.error(f"Error parsing XML {path} for {self.name}: {e}")
            return
        groups = {}
        for kid in dom.iterchildren():
            about = kid.get(RDF_ABOUT) or ""
            m = ARK.match(about)
            if not m:
                logger.debug(f"{self.name}: skipping node about {about!r}")
                continue
            groups.setdefault(m.group(1), []).append(kid)
        if len(remaining) == 1:
            self._add_total(len(groups))
        for kids in groups.values():
            root = etree.Element(dom.tag, nsmap=dom.nsmap)
            for k in kids:
                root.append(k)
            xml = etree.tostring(root, encoding="unicode")
            yield {"xml": xml.replace("http://data.bnf.fr/", "https://data.bnf.fr/")}

    def extract_identifier(self, data):
        xml = data.get("xml", "") if isinstance(data, dict) else ""
        m = ABOUT_RE.search(xml)
        return m.group(1) if m else None
