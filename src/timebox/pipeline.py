"""Thin orchestration: loader → normalizer → timestep → merge → budget → cursor
→ keyer → store.  No business decisions live here; the stage order is fixed and
each stage is a single-responsibility module.

The fixture flow deliberately keeps its original observable behaviour
(``{"fed", "rows"}``, idempotent upserts).  Backfill is a separate, explicitly
invoked flow (see :mod:`timebox.backfill` and the ``backfill`` CLI command).
"""
import importlib

from . import store as _store
from . import loader as _loader
from .keyer import key_of
from .models import Record

CHAIN_NAMES = ["normalizer", "timestep", "merge", "budget", "cursor"]


def run(con=None, *, observe=False):
    own = con is None
    con = con or _store.connect()
    try:
        chain = []
        for n in CHAIN_NAMES:
            mod = importlib.import_module("." + n, __package__)
            chain.append(mod.process)
        fed = 0
        for raw in _loader.load():
            original = dict(raw)
            rec = raw
            for fn in chain:
                rec = fn(rec)
            key = key_of(rec)
            _store.upsert(con, Record(key=key, data=rec))
            if observe:
                _store.record_observation(con, key, original)
            fed += 1
        _store.commit(con)
        return {"fed": fed, "rows": _store.count(con)}
    finally:
        if own:
            con.close()
