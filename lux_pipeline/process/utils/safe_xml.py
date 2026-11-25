"""lxml for XML from outside: entities left alone, nothing fetched.

lxml's default parser still loads external parameter entities a document
declares in its own DTD (``<!DOCTYPE r [<!ENTITY % x SYSTEM
"file:///dev/zero">%x;]>``): one record like that from a dump or a fetch
hangs the worker reading it for good, or names local files in libxml2's
errors. Every parse of a record, a dump or a response goes through these
instead of `lxml.etree` directly. Predefined entities (``&amp;``) and
character references (``&#233;``) are read as always; an entity a document
declares itself is left as it is, not expanded.

Each call makes its own parser: lxml parsers are not safe to share between
threads, and making one costs microseconds.
"""

from lxml import etree

SAFE = {"resolve_entities": False, "no_network": True}


def parser(**kw):
    """An `etree.XMLParser` with `SAFE` (and `kw`: ``remove_blank_text``,
    ``recover``, ...)."""
    return etree.XMLParser(**SAFE, **kw)


def XML(text, **kw):
    """`etree.XML`, safely."""
    return etree.XML(text, parser(**kw))


def fromstring(text, **kw):
    """`etree.fromstring`, safely."""
    return etree.fromstring(text, parser(**kw))


def parse(source, **kw):
    """`etree.parse`, safely."""
    return etree.parse(source, parser(**kw))


def iterparse(source, **kw):
    """`etree.iterparse` (``events``, ``tag``, ...), safely."""
    return etree.iterparse(source, **SAFE, **kw)


def XMLPullParser(**kw):
    """`etree.XMLPullParser` (``events``, ...), safely."""
    return etree.XMLPullParser(**SAFE, **kw)
