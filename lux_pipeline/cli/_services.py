"""Probes for the two servers an installation needs, Postgres and Redis,
using the same settings the stores use: the cache defaults record for
Postgres (``host`` empty means the local socket; ``user`` / ``dbname``
empty mean the operating-system user, as libpq does) and each map
record merged with the map defaults for Redis. `lux initialize` reports
them after installing; `lux testinstall` checks them before the idmap.
"""

import getpass
import os

import psycopg2
import redis

PLACEHOLDERS = ("USERNAME", "PASSWORD", "DBNAME")


def postgres_settings(cache_defaults):
    """``{host, port, user, password, dbname}`` from a cache defaults
    record (``caches.json`` or ``default_cache.json``), placeholders and
    blanks resolved the way the stores and libpq resolve them."""
    d = cache_defaults or {}
    osuser = os.getenv("USER") or getpass.getuser()

    def clean(key, default):
        v = d.get(key)
        return default if not v or v in PLACEHOLDERS else v

    return {"host": clean("host", ""), "port": int(d.get("port") or 5432),
            "user": clean("user", osuser), "password": clean("password", ""),
            "dbname": clean("dbname", osuser)}


def describe_postgres(s):
    where = f"{s['host']}:{s['port']}" if s["host"] else "local socket"
    return f"{where}, database {s['dbname']}, user {s['user']}"


def probe_postgres(settings, timeout=5):
    """Connect and ask the server its version; raises on failure."""
    kw = {"user": settings["user"], "dbname": settings["dbname"], "connect_timeout": timeout}
    if settings.get("host"):
        kw["host"] = settings["host"]
        kw["port"] = settings["port"]
    if settings.get("password"):
        kw["password"] = settings["password"]
    conn = psycopg2.connect(**kw)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT version()")
            version = cur.fetchone()[0].split(",")[0]
    finally:
        conn.close()
    return f"{version} at {describe_postgres(settings)}"


def redis_settings(map_record, map_defaults=None):
    """``{host, port, db}`` for a map record over the map defaults."""
    merged = dict(map_defaults or {})
    merged.update({k: v for k, v in (map_record or {}).items() if v is not None})
    return {"host": merged.get("host") or "localhost", "port": int(merged.get("port") or 6379),
            "db": int(merged.get("db") or 0)}


def redis_targets(map_records, map_defaults=None):
    """Distinct ``(host, port)`` of every map record whose storeClass is a
    redis store; empty when the configuration uses none."""
    seen = []
    for rec in map_records:
        cls = (rec or {}).get("storeClass") or (map_defaults or {}).get("storeClass") or ""
        if "redis" not in str(cls).lower():
            continue
        s = redis_settings(rec, map_defaults)
        key = (s["host"], s["port"])
        if key not in seen:
            seen.append(key)
    return seen


def probe_redis(host, port, timeout=5):
    """PING and the server version; raises on failure."""
    r = redis.Redis(host=host, port=port, socket_connect_timeout=timeout, socket_timeout=timeout)
    try:
        r.ping()
        version = r.info("server").get("redis_version", "?")
    finally:
        try:
            r.close()
        except Exception:
            pass
    return f"redis {version} at {host}:{port}"


def _pg_connect(settings, dbname=None, timeout=5):
    kw = {"user": settings["user"], "dbname": dbname or settings["dbname"], "connect_timeout": timeout}
    if settings.get("host"):
        kw["host"] = settings["host"]
        kw["port"] = settings["port"]
    if settings.get("password"):
        kw["password"] = settings["password"]
    return psycopg2.connect(**kw)


def postgres_database_status(settings, name, timeout=5):
    """``(exists, can_create)`` for database `name` as seen by the
    connecting role: whether it exists, and whether the role may create
    databases. Tries the configured database, then ``postgres``."""
    last = None
    for dbname in (settings["dbname"], "postgres"):
        try:
            conn = _pg_connect(settings, dbname, timeout)
        except Exception as e:
            last = e
            continue
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (name,))
                exists = cur.fetchone() is not None
                cur.execute("SELECT rolcreatedb OR rolsuper FROM pg_roles WHERE rolname = current_user")
                row = cur.fetchone()
                can_create = bool(row and row[0])
            return exists, can_create
        finally:
            conn.close()
    raise last


def create_database(settings, name, timeout=5):
    """``CREATE DATABASE name`` through the configured or the ``postgres``
    database (autocommit, as Postgres requires)."""
    last = None
    for dbname in (settings["dbname"], "postgres"):
        try:
            conn = _pg_connect(settings, dbname, timeout)
        except Exception as e:
            last = e
            continue
        try:
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute(f'CREATE DATABASE "{name}"')
            return True
        finally:
            conn.close()
    raise last
