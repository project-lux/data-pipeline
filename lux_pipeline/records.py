"""What is this record, and where is it? Any reference an operator can
type, followed through every cache and the identity map.

    ref = resolve(cfgs, "http://vocab.getty.edu/aat/300033618")
    ref = resolve(cfgs, "aat:300033618")             # a curie
    ref = resolve(cfgs, "obj/12345", source="ycba")  # a bare identifier
    ref = resolve(cfgs, "7f3a...")                    # a YUID (bare UUID or full URI)
    j = journey(cfgs, ref)        # datacache, recordcache, reconciled, identity, rewritten, merged, marklogic
    keys = sample(cfgs.external["aat"]["recordcache"], n=20, typ="Type")

What `lux interactive`, `lux viz` and ``manage-data.py --debug-record``
did by hand, for `lux show` and the web UI's record pages. Everything is
read through the cache contract (`storage/cache/abstract.py`) and the
idmap's lookups, so the filesystem stores of a test installation and the
Postgres stores of the build machine answer the same way.

A look never changes anything: nothing here stores a record, and
`journey(..., mapped=True)` maps the stored raw record with the source's
own mapper without fetching or storing (what that mapper reads of other
sources it reads as in a build).

Keys, as the pipeline writes them (refactor_docs/42 will change the first):

* a datacache row is keyed by the identifier;
* an internal source's recordcache by the identifier, an external one's
  by ``<identifier>##qua<Type>``;
* the identity map by the typed URI, ``<uri>##qua<Type>``, giving the
  YUID as a full URI (``<internal_uri><slug>/<uuid>``), and by that YUID,
  giving its members;
* recordcache2 (rewritten) and the merged and marklogic caches by the
  YUID's bare UUID.
"""

import copy
import itertools
import logging
import re
from dataclasses import asdict, dataclass

logger = logging.getLogger("lux_pipeline")

GROUPS = ("internal", "external", "results")
# curie prefixes that name LUX's own records (Config.split_curie)
YUID_PREFIXES = ("yuid", "final", "merged", "ml", "marklogic")
UUID_RE = re.compile(r"^[0-9a-fA-F]{8}-?[0-9a-fA-F]{4}-?[0-9a-fA-F]{4}-?[0-9a-fA-F]{4}-?"
                     r"[0-9a-fA-F]{12}$")


@dataclass
class Ref:
    """A resolved reference. `kind` is how it was typed: ``uri``,
    ``curie``, ``identifier`` (a source's own) or ``yuid``."""
    kind: str
    text: str
    source: str | None = None       # the source's name; None for a YUID
    group: str | None = None        # internal | external | results
    identifier: str | None = None   # as the source's datacache keys it
    uri: str | None = None          # <namespace><identifier>
    rectype: str | None = None      # from ##qua, when given
    uuid: str | None = None         # a YUID's bare UUID

    def as_dict(self):
        return asdict(self)


def group_of(cfgs, name):
    for g in GROUPS:
        if name in getattr(cfgs, g):
            return g
    return None


def instantiate_source(cfgs, name):
    """Source `name`'s configuration with its components built, building
    them only the first time (`Config.instantiate` builds them again on
    every call, opening new connections). ValueError for an unknown name."""
    group = group_of(cfgs, name)
    if group is None:
        raise ValueError(f"Unknown source: {name}")
    cfg = getattr(cfgs, group)[name]
    if cfg.get("datacache") is None:
        cfgs.instantiate(name, group)
    return cfg


# --- resolving ----------------------------------------------------------------------------


def _clean_identifier(cfgs, cfg, ident):
    """The identifier as the source keys it: its mapper's fix_identifier
    when the source is built, the generic cleaning otherwise (without the
    error Config.fix_identifier logs for a source with no mapper yet)."""
    if cfg.get("mapper") is not None:
        return cfgs.fix_identifier(cfg, ident)
    ident = ident.strip().replace(" ", "").replace('"', "")
    if ident.endswith("/"):
        ident = ident[:-1]
    elif ident.endswith(".html"):
        ident = ident[:-len(".html")]
    return ident


def _split(cfgs, uri, depth=0):
    """``(source cfg, identifier)`` for a URI, as `Config.split_uri`
    finds it (the rewrites and match order are its), or None."""
    for bad, good in getattr(cfgs, "external_uri_rewrites", {}).items():
        if bad in uri:
            uri = uri.replace(bad, good)
            break
    uri = cfgs.pre_split_fix_uri(uri)
    pairs = cfgs._match_pairs if cfgs._match_pairs is not None else cfgs.build_match_index()
    for m, cfg in pairs:
        if m in uri:
            ident = uri.rsplit(m, 1)[1]
            if ident.startswith(("http://", "https://")) and depth < 3:
                return _split(cfgs, ident, depth + 1)
            ident = _clean_identifier(cfgs, cfg, ident)
            return (cfg, ident) if ident else None
    return None


def _yuid_ref(text, tail):
    uuid = tail.rstrip("/").rsplit("/", 1)[-1]
    if not UUID_RE.match(uuid):
        raise ValueError(f"{text!r}: {uuid!r} is not a YUID")
    return Ref(kind="yuid", text=text, uuid=uuid)


def resolve(cfgs, text, source=None):
    """`text` as a `Ref`: a YUID (a bare UUID, or a URI under
    ``internal_uri``), anything with ``##qua<Type>``, a URI of a
    configured source, a curie (``aat:300033618``, ``merged:<uuid>``), or
    -- with `source` -- a bare identifier of that source. ValueError
    saying what could not be made of it. Never fetches anything."""
    text = (text or "").strip()
    if not text:
        raise ValueError("nothing to look up")
    internal = getattr(cfgs, "internal_uri", "") or ""
    if UUID_RE.match(text):
        return Ref(kind="yuid", text=text, uuid=text)
    if internal and text.startswith(internal):
        return _yuid_ref(text, text[len(internal):])
    if "##qua" in text:
        base, rectype = cfgs.split_qua(text)[:2]
        if rectype not in cfgs.ok_record_types:
            raise ValueError(f"{text!r}: {rectype!r} is not a record type")
        ref = resolve(cfgs, base, source)
        if ref.kind == "yuid":
            raise ValueError(f"{text!r}: a YUID has no ##qua type")
        ref.text, ref.rectype = text, rectype
        return ref
    if text.startswith(("http://", "https://")):
        found = _split(cfgs, text)
        if found is None:
            raise ValueError(f"{text!r}: no configured source matches this URI")
        cfg, ident = found
        return Ref(kind="uri", text=text, source=cfg["name"], group=group_of(cfgs, cfg["name"]),
                   identifier=ident, uri=f"{cfg['namespace']}{ident}")
    if ":" in text:
        prefix, rest = text.split(":", 1)
        if prefix in YUID_PREFIXES:
            return _yuid_ref(text, rest)
        group = group_of(cfgs, prefix)
        if group is not None:
            cfg = getattr(cfgs, group)[prefix]
            ident = _clean_identifier(cfgs, cfg, rest)
            return Ref(kind="curie", text=text, source=prefix, group=group, identifier=ident,
                       uri=f"{cfg.get('namespace', '')}{ident}")
    if source:
        group = group_of(cfgs, source)
        if group is None:
            raise ValueError(f"Unknown source: {source}")
        cfg = getattr(cfgs, group)[source]
        ident = _clean_identifier(cfgs, cfg, text)
        return Ref(kind="identifier", text=text, source=source, group=group, identifier=ident,
                   uri=f"{cfg.get('namespace', '')}{ident}")
    raise ValueError(f"{text!r} is neither a URI, a curie (source:identifier) nor a YUID; "
                     f"give the source for a bare identifier")


# --- looking ------------------------------------------------------------------------------


def _types(cfgs):
    """Every type a key can carry (``make_qua`` folds Material, Language,
    ... into Type), in configuration order."""
    out = []
    for t in cfgs.ok_record_types:
        t2 = cfgs.parent_record_types.get(t, t)
        if t2 not in out:
            out.append(t2)
    return out


def _record_type(row):
    data = (row or {}).get("data")
    typ = data.get("type") if isinstance(data, dict) else None
    return typ if isinstance(typ, str) else None


def _lookup(cache, keys, notes, label):
    """``(key, row)`` for the first of `keys` the cache holds, or None.
    One query for several keys where the cache has ``get_multi``."""
    keys = [k for k in dict.fromkeys(keys) if k]
    if cache is None or not keys:
        return None
    try:
        if len(keys) == 1:
            row = cache.get(keys[0])
            return (keys[0], row) if row is not None else None
        found = cache.get_multi(keys)
        for k in keys:
            if found.get(k) is not None:
                return k, found[k]
    except Exception as e:
        notes.append(f"{label}: {type(e).__name__}: {e}")
    return None


def _entry(hit):
    return None if hit is None else {"key": hit[0], "row": hit[1]}


def record_keys(cfgs, ref, group, rectype=None):
    """The recordcache keys to try for `ref`, most likely first: the bare
    identifier for an internal (or results) source, the typed one for an
    external; every type when the type is not known."""
    typed = [cfgs.make_qua(ref.identifier, t) for t in ([rectype] if rectype else _types(cfgs))]
    return [*typed, ref.identifier] if group == "external" else [ref.identifier, *typed]


def _identity(cfgs, idmap, uri, types, notes):
    """``(yuid, members, typed uri)`` from the idmap, trying each of
    `types` until one is known."""
    keys = [cfgs.make_qua(uri, t) for t in types]
    try:
        if hasattr(idmap, "get_cluster"):
            for k in keys:
                yuid, members = idmap.get_cluster(k)
                if yuid:
                    return yuid, members, k
            return None
        if len(keys) > 1 and hasattr(idmap, "get_multi"):
            found = idmap.get_multi(keys)
            keys = [k for k in keys if found.get(k)]
        for k in keys:
            yuid = idmap.get(k)
            if yuid:
                return yuid, idmap.get(yuid), k
    except Exception as e:
        notes.append(f"identity: {type(e).__name__}: {e}")
    return None


def _members(members):
    """A cluster's members, sorted, without the update token."""
    return sorted(m for m in (members or ()) if not (m.startswith("__") and m.endswith("__")))


def _yuid_for_uuid(cfgs, idmap, uuid, merged_row, notes):
    """The full YUID URI for a bare UUID: the merged record's id, else
    each record kind's slug tried in the idmap."""
    rid = (merged_row or {}).get("data", {}).get("id") if merged_row else None
    if isinstance(rid, str) and rid.endswith(uuid):
        return rid
    base = cfgs.internal_uri
    for slug in dict.fromkeys(cfgs.ok_record_types.values()):
        cand = f"{base}{slug}/{uuid}"
        try:
            if idmap.get(cand):
                return cand
        except Exception as e:
            notes.append(f"identity: {type(e).__name__}: {e}")
            return None
    return None


def map_record(cfgs, cfg, row, rectype=None):
    """Map a stored raw record with the source's mapper, as the acquirer
    does after fetching, and return the record(s): a list. Nothing is
    fetched (the row is given) or stored (``store=False``)."""
    acquirer = cfg["acquirer"]
    row = copy.deepcopy(row)
    if acquirer.returns_multiple(row):
        recs = cfg["mapper"].transform_all(row)
        return [r for r in (acquirer.do_post_map(r, r["data"]["type"], store=False) for r in recs) if r]
    rectype = rectype if rectype in cfgs.ok_record_types else _record_type(row)
    rectype = rectype if rectype in cfgs.ok_record_types else None
    mapped = cfg["mapper"].transform(row, rectype)
    if mapped is None:
        return []
    mapped["identifier"] = row.get("identifier")
    out = acquirer.do_post_map(mapped, mapped["data"]["type"], store=False)
    return [out] if out else []


def journey(cfgs, ref, *, mapped=False, idmap=None):
    """Where `ref` is in the pipeline::

        {"ref": ref.as_dict(),
         "datacache", "recordcache", "reconciled": {"key", "row"} | None,   # the source's caches
         "mapped": {"records": [...]} | None,                                # with mapped=True
         "identity": {"yuid", "uuid", "key", "members": [...]} | None,
         "rewritten", "merged", "marklogic": {"key", "row"} | None,         # by the YUID's UUID
         "notes": [...]}

    A store that cannot answer becomes a note and a None, so this never
    raises once `ref` is resolved. The source must be built
    (`instantiate_source`), and the results for merged / marklogic; a
    cache that is not there is None. `idmap` defaults to
    ``cfgs.get_idmap()``; a long-lived process passes one with its memory
    cache off. Rows are as the caches return them (times as datetimes:
    ``json.dumps(..., default=str)``)."""
    out = {"ref": ref.as_dict(), "datacache": None, "mapped": None, "recordcache": None,
           "reconciled": None, "identity": None, "rewritten": None, "merged": None,
           "marklogic": None, "notes": []}
    notes = out["notes"]
    if idmap is None:
        try:
            idmap = cfgs.get_idmap()
        except Exception as e:
            notes.append(f"identity: no idmap: {type(e).__name__}: {e}")
    cfg = None
    uuid = ref.uuid
    if ref.kind != "yuid":
        cfg = getattr(cfgs, ref.group)[ref.source]
        dc = _lookup(cfg.get("datacache"), [ref.identifier], notes, "datacache")
        out["datacache"] = _entry(dc)
        hint = ref.rectype or _record_type(dc[1] if dc else None)
        hint = hint if hint in cfgs.ok_record_types else None
        rc = _lookup(cfg.get("recordcache"), record_keys(cfgs, ref, ref.group, hint), notes,
                     "recordcache")
        out["recordcache"] = _entry(rc)
        if rc and "##qua" in rc[0]:
            notes.append(f"recordcache: found under the typed key {rc[0]}")
        rec = _lookup(cfg.get("reconciledRecordcache"), record_keys(cfgs, ref, "external", hint),
                      notes, "reconciled")
        out["reconciled"] = _entry(rec)
        rectype = ref.rectype or _record_type(rc[1] if rc else None) or hint
        if rectype is not None:
            rectype = cfgs.parent_record_types.get(rectype, rectype)
        if mapped:
            if dc is None:
                notes.append("mapped: no datacache record to map")
            elif cfg.get("acquirer") is None or cfg.get("mapper") is None:
                notes.append("mapped: the source has no acquirer or mapper")
            else:
                try:
                    out["mapped"] = {"records": map_record(cfgs, cfg, dc[1], rectype)}
                    if not out["mapped"]["records"]:
                        notes.append("mapped: the mapper made no record of it")
                except Exception as e:
                    notes.append(f"mapped: {type(e).__name__}: {e}")
        if idmap is not None:
            types = [rectype] if rectype in _types(cfgs) else _types(cfgs)
            found = _identity(cfgs, idmap, ref.uri, types, notes)
            if found:
                yuid, members, key = found
                uuid = yuid.rstrip("/").rsplit("/", 1)[-1]
                out["identity"] = {"yuid": yuid, "uuid": uuid, "key": key,
                                   "members": _members(members)}
    results = getattr(cfgs, "results", {})
    merged_cache = (results.get("merged") or {}).get("recordcache")
    if uuid:
        out["merged"] = _entry(_lookup(merged_cache, [uuid], notes, "merged"))
        if ref.kind == "yuid" and idmap is not None:
            row = out["merged"]["row"] if out["merged"] else None
            yuid = _yuid_for_uuid(cfgs, idmap, uuid, row, notes)
            if yuid:
                try:
                    members = idmap.get(yuid)
                except Exception as e:
                    members = None
                    notes.append(f"identity: {type(e).__name__}: {e}")
                out["identity"] = {"yuid": yuid, "uuid": uuid, "key": yuid,
                                   "members": _members(members)}
        if cfg is not None:
            out["rewritten"] = _entry(_lookup(cfg.get("recordcache2"), [uuid], notes, "rewritten"))
        ml_cache = (results.get("marklogic") or {}).get("recordcache")
        out["marklogic"] = _entry(_lookup(ml_cache, [uuid], notes, "marklogic"))
    if out["identity"] is None and idmap is not None:
        notes.append("identity: not in the identity map")
    return out


def metadata(cache, key):
    """``{field: value}`` of a record's metadata (insert / record /
    refresh time, valid, change), the fields the cache has a value for."""
    from lux_pipeline.storage.cache.abstract import METADATA_FIELDS
    out = {}
    for field in METADATA_FIELDS:
        try:
            md = cache.metadata(key, field)
        except Exception:
            md = None
        if md and md.get(field) is not None:
            out[field] = md[field]
    return out


def sample(cache, n=50, typ=None, since=None):
    """Up to `n` keys of `cache`: of records of type `typ` (``Concept``
    is any concept type; this reads records, so it scans the cache), or
    with a record time of at least `since`, or any.

    Nothing is left open afterwards. A Postgres cache iterates through a
    server-side cursor on a second connection (``iterating_conn``); stopping
    after `n` keys closes the cursor (closing the generator) but would leave
    that connection idle in its transaction, holding the table's ACCESS
    SHARE lock (a later TRUNCATE or CLUSTER of the table then fails), and
    ``end_read`` ends only the main connection's. So that transaction is
    rolled back here, where the iteration is known to be over; not in
    ``end_read``, which a build calls while a slice may still be
    iterating."""
    it = None
    try:
        if typ:
            it = cache.iter_keys_type(typ)
        elif since:
            it = cache.iter_keys_since(since)
        else:
            it = cache.iter_keys()
        return [str(k) for k in itertools.islice(it, max(0, int(n)))]
    finally:
        close = getattr(it, "close", None)
        if close is not None:
            close()
        end = getattr(cache, "end_read", None)
        if end is not None:
            end()
        conn = getattr(cache, "iterating_conn", None)
        if conn is not None and not getattr(conn, "autocommit", True):
            try:
                conn.rollback()
            except Exception as e:  # a look must not fail on tidying up
                logger.debug(f"ending the iteration's transaction: {e}")


def validate(cfgs, row):
    """Validation errors of a record row (``{"data": ...}``) as lines,
    ``[]`` when it is valid. FileNotFoundError when the validator's
    schemas are missing; ValueError for a record of a class the schemas do
    not cover."""
    v = getattr(cfgs, "validator", None)
    if v is None:
        raise ValueError("no validator configured (validatorClass in base)")
    if not isinstance(row, dict) or "data" not in row:
        row = {"data": row}
    return [f"/{'/'.join(str(x) for x in e.absolute_path)} --> {e.message}" for e in v.validate(row)]


def identity_graph(cfgs, uri_a, uri_b):
    """How reconciliation connected two URIs of one identity: `lux viz`'s
    graph as data (`cli.viz.graph_data`)."""
    from lux_pipeline.cli.viz import graph_data
    return graph_data(cfgs, uri_a, uri_b)
