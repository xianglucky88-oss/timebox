"""Local sqlite3 persistence: idempotent upserts, retained input history,
and durable engine state.

Three tables:

``records``
    Current merged view, idempotent by stable key (unchanged contract).
``observations``
    Append-only input history.  Every ingested raw record is retained with its
    content digest, so an overwrite in ``records`` never destroys the old
    input.  Re-ingesting identical input is a no-op.
``engine_state``
    JSON snapshots of backfill state (slice ledger, cursor, budget, merge
    history), used to resume after a restart.
"""
import hashlib
import json
import sqlite3

from . import config


def connect(db_path=None):
    path = db_path if db_path is not None else config.DB_PATH
    con = sqlite3.connect(str(path))
    con.execute(
        "CREATE TABLE IF NOT EXISTS records(key TEXT PRIMARY KEY, data TEXT NOT NULL)"
    )
    con.execute(
        "CREATE TABLE IF NOT EXISTS observations("
        "digest TEXT PRIMARY KEY, key TEXT NOT NULL, data TEXT NOT NULL, seq INTEGER)"
    )
    con.execute(
        "CREATE TABLE IF NOT EXISTS engine_state(key TEXT PRIMARY KEY, data TEXT NOT NULL)"
    )
    con.execute(
        "CREATE INDEX IF NOT EXISTS idx_observations_key ON observations(key)"
    )
    return con


def upsert(con, record):
    con.execute(
        "INSERT INTO records(key,data) VALUES(?,?) "
        "ON CONFLICT(key) DO UPDATE SET data=excluded.data",
        (record.key, json.dumps(record.data, ensure_ascii=False, sort_keys=True)),
    )


def record_observation(con, key, data):
    """Append a raw input to history; identical inputs are idempotent."""
    blob = json.dumps(data, ensure_ascii=False, sort_keys=True)
    digest = hashlib.sha256((str(key) + "\x00" + blob).encode("utf-8")).hexdigest()[:32]
    seq = con.execute("SELECT COALESCE(MAX(seq), 0) + 1 FROM observations").fetchone()[0]
    con.execute(
        "INSERT OR IGNORE INTO observations(digest,key,data,seq) VALUES(?,?,?,?)",
        (digest, str(key), blob, seq),
    )
    return digest


def observation_history(con, key=None):
    if key is None:
        rows = con.execute(
            "SELECT digest,key,data,seq FROM observations ORDER BY seq"
        ).fetchall()
    else:
        rows = con.execute(
            "SELECT digest,key,data,seq FROM observations WHERE key=? ORDER BY seq",
            (str(key),),
        ).fetchall()
    return [
        {"digest": d, "key": k, "data": json.loads(v), "seq": s}
        for d, k, v, s in rows
    ]


def save_state(con, key, state):
    con.execute(
        "INSERT INTO engine_state(key,data) VALUES(?,?) "
        "ON CONFLICT(key) DO UPDATE SET data=excluded.data",
        (str(key), json.dumps(state, ensure_ascii=False, sort_keys=True)),
    )


def load_state(con, key):
    row = con.execute("SELECT data FROM engine_state WHERE key=?", (str(key),)).fetchone()
    return json.loads(row[0]) if row else None


def commit(con):
    con.commit()


def count(con):
    return con.execute("SELECT COUNT(*) FROM records").fetchone()[0]


def all_records(con):
    return [(k, json.loads(d)) for k, d in con.execute("SELECT key,data FROM records")]


def get_record(con, key):
    row = con.execute("SELECT data FROM records WHERE key=?", (str(key),)).fetchone()
    return json.loads(row[0]) if row else None
