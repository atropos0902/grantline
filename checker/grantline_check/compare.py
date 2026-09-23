"""Compare two live copies of a table, one key range at a time.

A difference seen once proves nothing: the target is always a little behind. So
a range that differs is put back in line and read again after the target has
passed the point the source was at when it was read, and only the rows that
differed are looked at: a row that is equal once has nothing wrong with it, and
in a busy range some other row is always on its way. What still differs after
enough of those rounds, in the same rows and with the same values on both sides,
is reported as different. A range that keeps changing under us is reported as not settled. It is never
counted as equal.
"""

from __future__ import annotations

import hashlib
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Protocol

Key = tuple
Rows = dict[Key, bytes]


class Side(Protocol):
    """One copy of the table."""

    def bounds(self, lo: Key | None, n: int) -> Key | None:
        """The key n rows past lo, or None when the table ends first."""

    def read(self, lo: Key | None, hi: Key | None) -> Rows:
        """Rows with lo < key <= hi, as a digest of the row by key."""

    def position(self) -> int:
        """The source's log position, or the position the target has reached."""


@dataclass
class Range:
    lo: Key | None
    hi: Key | None
    tries: int = 0
    wait_for: int = 0
    last: frozenset = frozenset()
    same_again: int = 0
    suspects: frozenset | None = None  # keys that have differed every time so far


@dataclass
class Report:
    rows: int = 0
    ranges: int = 0
    compared_again: int = 0
    missing: list[Key] = field(default_factory=list)
    extra: list[Key] = field(default_factory=list)
    different: list[Key] = field(default_factory=list)
    not_settled: list[tuple[Key | None, Key | None]] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not (self.missing or self.extra or self.different or self.not_settled)


def digest(text: str) -> bytes:
    return hashlib.blake2b(text.encode(), digest_size=16).digest()


def differences(source: Rows, target: Rows) -> frozenset[tuple]:
    """What differs, with both values: a row that differs again only counts as
    the same difference when neither side has changed."""
    out = set()
    for k in source.keys() | target.keys():
        a, b = source.get(k), target.get(k)
        if a != b:
            out.add(("missing" if b is None else "extra" if a is None else "different", k, a, b))
    return frozenset(out)


def compare(
    source: Side,
    target: Side,
    *,
    rows: int = 10_000,
    rounds: int = 20,
    sure_after: int = 3,
    pause: float = 0.2,
    sleep=time.sleep,
) -> Report:
    """Compare the two sides. sure_after is how many rounds in a row the same
    rows have to differ before they are called different."""
    report = Report()
    todo: deque[Range] = deque()
    lo: Key | None = None
    while True:
        hi = source.bounds(lo, rows)
        todo.append(Range(lo, hi))
        report.ranges += 1
        if hi is None:
            break
        lo = hi

    while todo:
        r = todo.popleft()
        if r.wait_for and target.position() < r.wait_for:
            # Not there yet. If nothing else is in line, wait for it.
            r.tries += 1
            if r.tries > rounds * 50:
                report.not_settled.append((r.lo, r.hi))
                continue
            todo.append(r)
            sleep(pause / 10 if len(todo) > 1 else pause)
            continue
        # Target first: anything it shows was committed at the source before
        # the source is read.
        t = target.read(r.lo, r.hi)
        s = source.read(r.lo, r.hi)
        found = differences(s, t)
        if r.suspects is not None:
            found = frozenset(d for d in found if d[1] in r.suspects)
        r.suspects = frozenset(d[1] for d in found)
        if not found:
            report.rows += len(s)
            continue
        r.same_again = r.same_again + 1 if found == r.last else 1
        r.last = found
        r.tries += 1
        report.compared_again += 1
        if r.same_again >= sure_after:
            report.rows += len(s)
            for what, key, _, _ in sorted(found, key=repr):
                getattr(report, what).append(key)
            continue
        if r.tries >= rounds:
            report.not_settled.append((r.lo, r.hi))
            continue
        r.wait_for = source.position()
        todo.append(r)
        sleep(pause)
    return report
