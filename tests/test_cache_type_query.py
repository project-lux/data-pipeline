"""The record type in PooledCache's type queries is bound as a parameter,
never pasted into the SQL: the web UI's browse page passes it from a URL."""

from contextlib import contextmanager

import pytest
import ujson

from lux_pipeline.storage.cache.postgres import PooledCache

HOSTILE = "x\"'; CREATE TEMP TABLE pwned AS SELECT current_user AS u --"


def cache(rows):
    c = object.__new__(PooledCache)
    c.name, c.key = "aat_record_cache", "identifier"
    c.executed = []

    class Cursor(list):
        def execute(self, qry, params=None):
            c.executed.append((qry, params))

    @contextmanager
    def cursor(internal=True, iter=False, size=0):
        yield Cursor(rows)

    c._cursor = cursor
    return c


CALLS = [("iter_keys_type", ()), ("iter_records_type", ()), ("iter_keys_type_slice", (0, 1))]


@pytest.mark.parametrize("method,extra", CALLS)
@pytest.mark.parametrize("typ", ["Person", HOSTILE])
def test_the_type_is_a_bound_parameter(method, extra, typ):
    c = cache([{"identifier": "k"}])
    list(getattr(c, method)(typ, *extra))
    [(qry, params)] = c.executed
    assert "%s::jsonb" in qry and typ not in qry
    # the same jsonb value the query compared with before: '"Person"'
    assert params == (ujson.dumps(typ),) and ujson.loads(params[0]) == typ


@pytest.mark.parametrize("method,extra", CALLS)
def test_concept_is_every_concept_type(method, extra):
    c = cache([])
    list(getattr(c, method)("Concept", *extra))
    [(qry, params)] = c.executed
    assert "IN ('Type', 'Currency', 'Language', 'Material', 'MeasurementUnit')" in qry
    assert params is None
