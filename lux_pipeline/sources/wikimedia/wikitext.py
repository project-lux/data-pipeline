"""Just enough wikitext for Wikimedia Commons file pages: their templates,
the licence those templates grant, and the Information fields as plain
text. No wikitext library is a dependency, and file pages are regular
enough not to need one.

A licence is a template anywhere on the page: in the ``{{int:license-header}}``
section (``{{self|cc-by-sa-4.0}}``, ``{{cc-zero}}``, ``{{PD-old-70}}``) or
inside another template (``|permission={{PD-FLGov-PhotoColl}}``,
``{{Licensed-PD-Art|1=PD-old-auto|...}}``). `page_licenses` gives the
codes the Commons API reports as ``License``, so `WmMapper` reads a page
loaded from a dump and one fetched from the API the same way:

* ``pd``: ``PD``, ``PD-*`` (``PD-self``, ``PD-old-70``, ``PD-USGov-*``,
  ``PD-Art``), ``Licensed-PD-*``, ``CC-PD-Mark``, ``Public domain``;
* ``cc0``: ``CC-zero``, ``CC0``;
* ``cc-by-4.0`` / ``cc-by-sa-4.0``: ``cc-by[-sa]-4.0``, or a version list
  that includes 4.0 (``cc-by-sa-4.0,3.0,2.5``), or ``cc-by[-sa]-all``; also as an argument of
  ``{{self|...}}``.

Anything else (GFDL, 3.0 and older Creative Commons licences, Flickr
review stamps) grants nothing here. A page flagged for deletion or with
its licence or source disputed (`FLAGGED`) grants nothing either.
"""

import re
from collections import namedtuple
from html import unescape

# the codes, in the order a page with several reports one: the least
# restrictive first
LICENSES = {
    "pd": {"short": "Public domain", "url": "", "terms": "Public domain"},
    "cc0": {"short": "CC0", "url": "http://creativecommons.org/publicdomain/zero/1.0/deed.en",
            "terms": "Creative Commons Zero, Public Domain Dedication"},
    "cc-by-4.0": {"short": "CC BY 4.0", "url": "https://creativecommons.org/licenses/by/4.0",
                  "terms": "Creative Commons Attribution 4.0"},
    "cc-by-sa-4.0": {"short": "CC BY-SA 4.0", "url": "https://creativecommons.org/licenses/by-sa/4.0",
                     "terms": "Creative Commons Attribution-Share Alike 4.0"},
}

# a page with one of these is up for deletion or its licence is in doubt
FLAGGED = frozenset({
    "delete", "copyvio", "speedy", "speedydelete", "speedy-delete",
    "no-license", "no-license-since", "no-permission-since", "no-source-since",
    "nld", "npd", "nsd", "dw-no-source-since",
})

IMAGE_EXTENSIONS = ("jpg", "jpeg", "png", "gif", "tif", "tiff", "webp", "svg")

# the templates whose fields describe the file
INFO_TEMPLATES = ("information", "artwork", "photograph", "art-photo", "book", "map")

Template = namedtuple("Template", "name positional named raw")

_BRACES = re.compile(r"\{\{|\}\}")
_SPLIT = re.compile(r"\{\{|\}\}|\[\[|\]\]|\|")
_COMMENT = re.compile(r"<!--.*?-->", re.S)
_CC = re.compile(r"^cc-(by(?:-sa)?)-((?:\d\.\d,?)+)(?:-[a-z]{2,3})?$")
_LANG = re.compile(r"^[a-z]{2,3}(?:-[a-z]{2,8})?$")
_SPACE = re.compile(r"[\s_]+")
_TAG = re.compile(r"</?[a-zA-Z][^<>]*>")


def norm(name):
    """A template name as compared here: lower case, ``template:`` gone,
    runs of spaces and underscores as one ``-``."""
    name = _SPACE.sub("-", name.strip().lower())
    if name.startswith("template:"):
        name = name[9:]
    return name.strip("-")


def strip_comments(text):
    return _COMMENT.sub("", text or "")


def template_spans(text):
    """``(start, end)`` of each outermost ``{{...}}`` in `text`;
    unbalanced braces end the scan."""
    spans = []
    depth = 0
    start = 0
    for m in _BRACES.finditer(text):
        if m.group() == "{{":
            if depth == 0:
                start = m.start()
            depth += 1
        elif depth:
            depth -= 1
            if depth == 0:
                spans.append((start, m.end()))
    return spans


def split_args(body):
    """`body` split at the ``|`` not inside a nested template or link."""
    parts = []
    depth = 0
    last = 0
    for m in _SPLIT.finditer(body):
        tok = m.group()
        if tok in ("{{", "[["):
            depth += 1
        elif tok in ("}}", "]]"):
            depth = max(0, depth - 1)
        elif depth == 0:
            parts.append(body[last:m.start()])
            last = m.end()
    parts.append(body[last:])
    return parts


def parse_template(body):
    """A `Template` from the text between ``{{`` and ``}}``."""
    parts = split_args(body)
    positional, named = [], {}
    for p in parts[1:]:
        key, eq, value = p.partition("=")
        if eq and key.strip() and "{{" not in key and "[[" not in key:
            named[key.strip().lower()] = value.strip()
        else:
            positional.append(p.strip())
    return Template(norm(parts[0]), positional, named, parts[0].strip())


def templates(text):
    """Every template in `text`, nested ones too, outer before inner."""
    out = []
    for start, end in template_spans(text):
        body = text[start + 2:end - 2]
        out.append(parse_template(body))
        out.extend(templates(body))
    return out


def license_code(name):
    """The licence code a (normalized) template name grants, or None."""
    if name in ("cc-zero", "cc0", "cc-0") or name.startswith(("cc-zero-", "cc0-")):
        return "cc0"
    if (name == "pd" or name.startswith(("pd-", "licensed-pd", "cc-pd"))
            or name in ("public-domain", "publicdomain")):
        return "pd"
    if name in ("cc-by-all", "cc-by-sa-all"):
        # every version, 4.0 among them
        return "cc-by-4.0" if name == "cc-by-all" else "cc-by-sa-4.0"
    m = _CC.match(name)
    if m and "4.0" in m.group(2).split(","):
        return "cc-by-4.0" if m.group(1) == "by" else "cc-by-sa-4.0"
    return None


def page_licenses(text, allowed=tuple(LICENSES)):
    """The codes in `allowed` the page's templates grant, least restrictive
    first; empty for a page flagged by a `FLAGGED` template."""
    found = set()
    for t in templates(strip_comments(text)):
        if t.name in FLAGGED:
            return []
        names = [t.name]
        if t.name in ("self", "self2"):
            names += [norm(a) for a in t.positional]
        for n in names:
            code = license_code(n)
            if code:
                found.add(code)
    order = list(LICENSES) + [c for c in allowed if c not in LICENSES]
    return [c for c in order if c in found and c in allowed]


def is_image(title, extensions=IMAGE_EXTENSIONS):
    """Whether a ``File:`` title names an image, by its extension."""
    ext = title.rsplit(".", 1)[-1].lower() if "." in title else ""
    return ext in extensions


# --- fields as plain text -------------------------------------------------------

def _render_template(t, lang):
    """The text a template stands for here, or ''."""
    if t.name in ("own", "own-work"):
        return "Own work"
    if t.name.startswith("creator:"):
        return t.raw.split(":", 1)[1].strip()
    if t.name in ("u", "user", "w", "city", "label"):
        args = t.positional
        return render(args[1] if len(args) > 1 else args[0], lang) if args else ""
    if t.name == "date" and t.positional:
        return "-".join(a for a in t.positional[:3] if a)
    return ""


def render(text, lang="en"):
    """Wikitext as plain text: a multilingual value as its `lang` part
    (else the first), templates as the text they stand for or nothing,
    links as their labels, no bold or italic marks or HTML tags, entities
    decoded, whitespace collapsed."""
    text = strip_comments(text)
    spans = template_spans(text)
    parsed = [parse_template(text[s + 2:e - 2]) for s, e in spans]
    langs = [(t, s) for t, s in zip(parsed, spans)
             if _LANG.match(t.name) and (t.named.get("1") or t.positional)]
    if langs:
        pick = next((t for t, _ in langs if t.name == lang), langs[0][0])
        return render(pick.named.get("1") or pick.positional[0], lang)
    out, last = [], 0
    for t, (s, e) in zip(parsed, spans):
        out.append(text[last:s])
        out.append(_render_template(t, lang))
        last = e
    out.append(text[last:])
    text = "".join(out)
    text = re.sub(r"\[\[(?:[^\]|]*\|)?([^\]]*)\]\]", r"\1", text)
    text = re.sub(r"\[(?:https?:)?//\S+\s+([^\]]*)\]", r"\1", text)
    text = re.sub(r"\[(?:https?:)?//\S+\]", "", text)
    text = text.replace("'''", "").replace("''", "")
    text = _TAG.sub(" ", text)
    return " ".join(unescape(text).split())


def file_fields(text, lang="en"):
    """The first Information-like template's ``title``, ``description``,
    ``author``, ``source`` and ``date`` as plain text, with ``artist`` and
    ``photographer`` read as ``author``; missing ones are absent."""
    for t in templates(strip_comments(text)):
        if t.name not in INFO_TEMPLATES:
            continue
        out = {}
        for key, names in (("title", ("title",)), ("description", ("description",)),
                           ("author", ("author", "artist", "photographer")),
                           ("source", ("source",)), ("date", ("date",))):
            for n in names:
                value = render(t.named.get(n, ""), lang)
                if value:
                    out[key] = value
                    break
        return out
    return {}
