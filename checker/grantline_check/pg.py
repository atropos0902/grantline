"""The two sides of the comparison, in Postgres."""

from __future__ import annotations

import psycopg
from psycopg import sql

from .compare import Key, Rows, digest

SETTINGS = "-c datestyle=ISO -c timezone=UTC -c extra_float_digits=3 -c bytea_output=hex -c intervalstyle=postgres"


def connect(dsn: str) -> psycopg.Connection:
    return psycopg.connect(dsn, autocommit=True, options=SETTINGS)


def lsn(text: str) -> int:
    hi, lo = text.split("/")
    return int(hi, 16) << 32 | int(lo, 16)


class Table:
    def __init__(self, conn: psycopg.Connection, schema: str, name: str, replication: str | None, columns: list[str] | None = None):
        self.conn, self.replication = conn, replication
        self.ident = sql.Identifier(schema, name)
        key = conn.execute(
            """select a.attname from pg_index i, unnest(i.indkey) with ordinality k(n, o), pg_attribute a
               where i.indrelid = to_regclass(%s) and i.indisprimary and a.attrelid = i.indrelid and a.attnum = k.n
               order by k.o""",
            (f'"{schema}"."{name}"',),
        ).fetchall()
        if not key:
            raise ValueError(f"{schema}.{name} has no primary key")
        self.key = [k for (k,) in key]
        self.columns = columns or [
            c
            for (c,) in conn.execute(
                """select attname from pg_attribute where attrelid = to_regclass(%s)
                   and attnum > 0 and not attisdropped and attgenerated = '' order by attname""",
                (f'"{schema}"."{name}"',),
            )
        ]
        self.types = dict(
            conn.execute(
                "select attname, format_type(atttypid, atttypmod) from pg_attribute where attrelid = to_regclass(%s) and attnum > 0",
                (f'"{schema}"."{name}"',),
            ).fetchall()
        )

    def _keys(self) -> sql.Composable:
        return sql.SQL(", ").join(map(sql.Identifier, self.key))

    def _side(self, op: str, k: Key) -> sql.Composable:
        vals = sql.SQL(", ").join(
            sql.SQL("{}::{}").format(sql.Literal(v), sql.SQL(self.types[c])) for c, v in zip(self.key, k, strict=True)
        )
        return sql.SQL("({}) {} ({})").format(self._keys(), sql.SQL(op), vals)

    def _where(self, lo: Key | None, hi: Key | None) -> sql.Composable:
        conds = [self._side(">", lo)] if lo else []
        conds += [self._side("<=", hi)] if hi else []
        return sql.SQL(" and ").join(conds) if conds else sql.SQL("true")

    def _text(self, cols: list[str]) -> sql.Composable:
        return sql.SQL(", ").join(sql.SQL("{}::text").format(sql.Identifier(c)) for c in cols)

    def bounds(self, lo: Key | None, n: int) -> Key | None:
        return self.conn.execute(
            sql.SQL("select {} from {} where {} order by {} offset {} limit 1").format(
                self._text(self.key), self.ident, self._where(lo, None), self._keys(), sql.Literal(n - 1)
            )
        ).fetchone()

    def read(self, lo: Key | None, hi: Key | None) -> Rows:
        n = len(self.key)
        cur = self.conn.execute(
            sql.SQL("select {}, {} from {} where {}").format(
                self._text(self.key), self._text(self.columns), self.ident, self._where(lo, hi)
            )
        )
        return {row[:n]: digest(repr(row[n:])) for row in cur}

    def position(self) -> int:
        if self.replication is None:
            # The insert position: a commit that doesn't wait for the disk is
            # visible before the write position has passed it.
            q, args = "select pg_current_wal_insert_lsn()::text", ()
        else:
            q, args = "select lsn::text from grantline.progress where replication = %s", (self.replication,)
        (at,) = self.conn.execute(q, args).fetchone()
        return lsn(at)
