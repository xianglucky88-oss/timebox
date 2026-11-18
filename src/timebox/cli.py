"""CLI: fixture ingest (default) plus explicit backfill/report commands.

Usage::

    python -m timebox.cli                      # fixture pipeline (unchanged)
    python -m timebox.cli backfill --start ... --end ... --unit day
    python -m timebox.cli status  --start ... --end ...
    python -m timebox.cli budget-explain --op-id ...
"""
import argparse
import json
import sys

from . import pipeline


def _parse(argv):
    parser = argparse.ArgumentParser(prog="timebox")
    sub = parser.add_subparsers(dest="command")

    common = dict(dest="rest", nargs=argparse.REMAINDER)

    bf = sub.add_parser("backfill", help="backfill a historical range from the fixture")
    bf.add_argument("--start", required=True)
    bf.add_argument("--end", required=True)
    bf.add_argument("--unit", default="day", choices=["hour", "day"])
    bf.add_argument("--tz", default="UTC")
    bf.add_argument("--precision", default="microsecond")
    bf.add_argument("--source-name", default="fixture")
    bf.add_argument("--state-key", default="fixture")
    bf.add_argument("--no-save", action="store_true")

    st = sub.add_parser("status", help="report coverage/unfilled/resume position")
    st.add_argument("--start", required=True)
    st.add_argument("--end", required=True)
    st.add_argument("--unit", default="day", choices=["hour", "day"])
    st.add_argument("--tz", default="UTC")
    st.add_argument("--precision", default="microsecond")
    st.add_argument("--source-name", default="fixture")
    st.add_argument("--state-key", default="fixture")

    return parser.parse_args(argv)


def _engine(args, con):
    from . import backfill, store
    from .backfill import StaticSource

    state = None if getattr(args, "no_save", False) else store.load_state(con, args.state_key)
    source = StaticSource(args.source_name, lambda iv, revision, attempt: _fixture_records(iv))
    if state:
        return backfill.BackfillEngine.load(state, source)
    return backfill.BackfillEngine(
        source, source_tz=args.tz, precision=args.precision, unit=args.unit
    )


def _fixture_records(iv):
    from .loader import load
    from .timestep import parse_instant

    out = []
    for rec in load():
        ts = rec.get("ts")
        if not ts:
            continue
        if iv.contains(parse_instant(ts, str(rec.get("source_tz") or "UTC"))):
            out.append(rec)
    return out


def main(argv=None):
    argv = list(sys.argv[1:] if argv is None else argv)
    if not argv:
        print(json.dumps(pipeline.run(), ensure_ascii=False))
        return 0

    args = _parse(argv)
    from . import store

    con = store.connect()
    try:
        if args.command == "backfill":
            engine = _engine(args, con)
            run = engine.run(args.start, args.end, unit=args.unit)
            if not args.no_save:
                store.save_state(con, args.state_key, engine.dump())
                _persist_entities(con, engine)
            store.commit(con)
            print(json.dumps({"run": run.as_dict(), "status": engine.status(args.start, args.end)}, ensure_ascii=False))
            return 0
        if args.command == "status":
            engine = _engine(args, con)
            print(json.dumps(engine.status(args.start, args.end), ensure_ascii=False))
            return 0
    finally:
        con.close()
    return 2


def _persist_entities(con, engine):
    from . import store
    from .keyer import key_of
    from .models import Record

    for entity, state in engine.merger.entities.items():
        if state.exists:
            store.upsert(con, Record(key=key_of({"id": entity}), data=dict(state.payload)))


if __name__ == "__main__":
    sys.exit(main())
