"""grantline-check --source DSN --target DSN --replication NAME table ..."""

from __future__ import annotations

import argparse
import json
import sys

from .compare import compare
from .pg import Table, connect


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="grantline-check", description=__doc__)
    p.add_argument("--source", required=True)
    p.add_argument("--target", required=True)
    p.add_argument("--replication", required=True)
    p.add_argument("--rows", type=int, default=10_000, help="rows in a range")
    p.add_argument("--rounds", type=int, default=20, help="reads before a range is called not settled")
    p.add_argument("--json", action="store_true")
    p.add_argument("tables", nargs="+")
    a = p.parse_args(argv)

    bad = False
    with connect(a.source) as sc, connect(a.target) as tc:
        for name in a.tables:
            schema, _, table = name.rpartition(".")
            s = Table(sc, schema or "public", table, None)
            t = Table(tc, schema or "public", table, a.replication, s.columns)
            r = compare(s, t, rows=a.rows, rounds=a.rounds)
            bad |= not r.ok
            if a.json:
                print(json.dumps({"table": name, "ok": r.ok, **r.__dict__}))
                continue
            print(f"{name}: {r.rows} rows in {r.ranges} ranges, {r.compared_again} compared again")
            for what in ("missing", "extra", "different", "not_settled"):
                for k in getattr(r, what)[:20]:
                    print(f"  {what.replace('_', ' ')}: {k}")
            print("  same" if r.ok else "  NOT the same")
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
