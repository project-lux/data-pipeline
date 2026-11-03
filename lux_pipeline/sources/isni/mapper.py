"""ISNI records (see ``downloader.py`` for how they are stored: one node of
the published JSON-LD, in its compact form) as Linked Art People and
Groups.

* ``@type`` ``schema:Person`` -> Person, ``schema:Organization`` -> Group
* the ISNI -> an identifier, shown in its grouped form
  (``0000 0001 2103 2683``)
* ``schema:name``, then ``schema:alternateName`` -> the primary name
  (the first) and alternate names; a ``@language`` the pipeline knows
  becomes the name's language
* ``owl:sameAs`` and ``madsrdf:isIdentifiedByAuthority`` -> equivalents,
  once each, in the order given
* ``schema:birthDate`` / ``schema:deathDate`` (people) and
  ``schema:foundingDate`` / ``schema:dissolutionDate`` (groups) -> birth,
  death, formation and dissolution, with ``make_datetime``'s range where
  it can parse the date and the text as given either way

The record's URI is the configured ``namespace`` plus the ISNI, so it is
the form other sources' equivalents use.
"""

from cromulent import model, vocab

from lux_pipeline.process.base.mapper import Mapper
from lux_pipeline.process.utils.mapper_utils import make_datetime
from lux_pipeline.sources.isni.downloader import isni_number

TYPES = {"schema:Person": model.Person, "Person": model.Person,
         "http://schema.org/Person": model.Person, "https://schema.org/Person": model.Person,
         "schema:Organization": model.Group, "Organization": model.Group,
         "http://schema.org/Organization": model.Group,
         "https://schema.org/Organization": model.Group}


def as_list(value):
    if value is None:
        return []
    return value if isinstance(value, list) else [value]


def literals(value):
    """``(text, language or None)`` for each of a JSON-LD value's strings:
    plain strings and ``{"@value", "@language"}`` objects."""
    out = []
    for v in as_list(value):
        if isinstance(v, dict):
            text, lang = v.get("@value"), v.get("@language")
        else:
            text, lang = v, None
        if isinstance(text, (int, float)) and not isinstance(text, bool):
            text = str(text)
        if isinstance(text, str) and text.strip():
            out.append((text.strip(), lang.lower() if isinstance(lang, str) else None))
    return out


def uris(value):
    """The URIs in a JSON-LD value: strings and ``{"@id"}`` objects."""
    out = []
    for v in as_list(value):
        if isinstance(v, dict):
            v = v.get("@id")
        if isinstance(v, str) and v.strip():
            out.append(v.strip())
    return out


def format_isni(isni):
    """``0000000121032683`` -> ``0000 0001 2103 2683``."""
    if len(isni) == 16:
        return " ".join(isni[i:i + 4] for i in range(0, 16, 4))
    return isni


class IsniMapper(Mapper):
    def guess_type(self, data):
        """Person or Group from ``@type``; Person when it says neither."""
        for t in as_list(data.get("@type")):
            if t in TYPES:
                return TYPES[t]
        return model.Person

    def make_timespan(self, text):
        ts = model.TimeSpan()
        try:
            dates = make_datetime(text)
        except Exception:      # the text is kept either way
            dates = None
        if dates:
            ts.begin_of_the_begin, ts.end_of_the_end = dates
        ts.identified_by = vocab.DisplayName(content=text)
        return ts

    def add_event(self, top, data, key, cls, prop):
        for text, _ in literals(data.get(key))[:1]:
            event = cls()
            event.timespan = self.make_timespan(text)
            setattr(top, prop, event)

    def add_person_dates(self, top, data):
        self.add_event(top, data, "schema:birthDate", model.Birth, "born")
        self.add_event(top, data, "schema:deathDate", model.Death, "died")

    def add_group_dates(self, top, data):
        self.add_event(top, data, "schema:foundingDate", model.Formation, "formed_by")
        self.add_event(top, data, "schema:dissolutionDate", model.Dissolution, "dissolved_by")

    def add_common(self, top, data, isni, cls):
        number = vocab.LocalNumber(content=format_isni(isni))
        assignment = model.AttributeAssignment()
        assignment.carried_out_by = model.Group(label="ISNI International Agency")
        number.assigned_by = assignment
        top.identified_by = number

        names = []
        for text, lang in literals(data.get("schema:name")) + literals(data.get("schema:alternateName")):
            if (text, lang) not in names:
                names.append((text, lang))
        for i, (text, lang) in enumerate(names):
            name = vocab.PrimaryName(content=text) if i == 0 else vocab.AlternateName(content=text)
            if lang:
                lang = self.lang_three_to_two.get(lang, lang)
                if lang in self.process_langs:
                    name.language = self.process_langs[lang]
            top.identified_by = name
        if names:
            top._label = names[0][0]

        seen = {top.id}
        for uri in uris(data.get("owl:sameAs")) + uris(data.get("madsrdf:isIdentifiedByAuthority")):
            if uri not in seen:
                seen.add(uri)
                top.equivalent = cls(ident=uri)

    def transform(self, record, rectype=None, reference=False):
        data = record.get("data", record)
        isni = isni_number(data.get("@id")) or isni_number(f"isni.org/isni/{record.get('identifier', '')}")
        if not isni:
            return None
        cls = self.guess_type(data)
        if rectype:
            wanted = rectype if isinstance(rectype, str) else rectype.__name__
            if wanted != cls.__name__:
                return None
        top = cls(ident=f"{self.namespace}{isni}", label=f"ISNI {format_isni(isni)}")
        self.add_common(top, data, isni, cls)
        if cls is model.Person:
            self.add_person_dates(top, data)
        else:
            self.add_group_dates(top, data)
        return {"identifier": isni, "data": model.factory.toJSON(top), "source": self.name}
