"""The reidentifier's "couldn't find YUID" line names the key it looked up.

The idmap's keys carry the class (``<uri>##qua<Class>``) and the lookup uses
the referencing node's type, so a reference typed differently from the
record identify registered misses although the URI has a YUID. The line
used to print only the bare URI, which made that look the same as a
reference no build reconciled.
"""

import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from lux_pipeline.process.reidentifier import Reidentifier


class Configs:
    def make_qua(self, uri, typ):
        return f"{uri}##qua{typ}"


class IdMap(dict):
    """None for a missing key, as the idmaps answer"""
    def __getitem__(self, key):
        return self.get(key)


def reidentifier(idmap):
    idmap = IdMap(idmap)
    r = object.__new__(Reidentifier)
    r.configs = Configs()
    r.idmap = idmap
    r.do_not_reidentify = []
    r.redirects = {}
    r.debug = False
    r.preserve_equivalents = {}
    return r


def test_a_miss_logs_the_qua_key(caplog):
    # registered as a Place; referenced as a Person
    r = reidentifier({"http://vocab.getty.edu/tgn/1002608##quaPlace": "yuid:place/1"})
    node = {"id": "http://vocab.getty.edu/tgn/1002608", "type": "Person"}
    with caplog.at_level(logging.ERROR, logger="lux_pipeline"):
        result = r.process_entity(node)
    assert "id" not in result
    assert "couldn't find YUID for http://vocab.getty.edu/tgn/1002608##quaPerson" in caplog.text


def test_a_hit_logs_nothing(caplog):
    r = reidentifier({"http://vocab.getty.edu/tgn/1002608##quaPlace": "yuid:place/1"})
    node = {"id": "http://vocab.getty.edu/tgn/1002608", "type": "Place"}
    with caplog.at_level(logging.ERROR, logger="lux_pipeline"):
        result = r.process_entity(node)
    assert result["id"] == "yuid:place/1"
    assert "couldn't find YUID" not in caplog.text
