from hypothesis import given, settings
from hypothesis import strategies as st

from grantline_check.compare import compare, digest

rows = st.dictionaries(st.integers(0, 60).map(lambda i: (i,)), st.text(max_size=3), max_size=40)


class Memory:
    """A copy held in memory. Each read of the target applies some of what is
    still on its way, so the two sides are never quite at the same point."""

    def __init__(self, rows, pos=0):
        self.rows, self.pos = dict(rows), pos

    def bounds(self, lo, n):
        ks = sorted(k for k in self.rows if lo is None or k > lo)
        return ks[n - 1] if len(ks) >= n else None

    def read(self, lo, hi):
        return {k: digest(v) for k, v in self.rows.items() if (lo is None or k > lo) and (hi is None or k <= hi)}

    def position(self):
        return self.pos


class Behind(Memory):
    def __init__(self, rows, changes, speed, churn=None):
        super().__init__(rows)
        self.changes, self.speed, self.churn = list(changes), speed, churn

    def step(self):
        for _ in range(self.speed):
            if self.changes:
                k, v = self.changes.pop(0)
                if v is None:
                    self.rows.pop(k, None)
                else:
                    self.rows[k] = v
                self.pos += 1

    def read(self, lo, hi):
        self.step()
        if self.churn:
            self.churn()
        return super().read(lo, hi)

    def position(self):
        self.step()
        return self.pos


def run(s, t, **kw):
    return compare(s, t, pause=0, sleep=lambda _: None, **kw)


@given(rows, st.integers(1, 7))
def test_equal_copies_are_equal(r, n):
    rep = run(Memory(r), Memory(r), rows=n)
    assert rep.ok and rep.rows == len(r)


@given(rows, rows, st.integers(1, 7))
def test_every_real_difference_is_found(a, b, n):
    rep = run(Memory(a), Memory(b), rows=n)
    assert set(rep.missing) == a.keys() - b.keys()
    assert set(rep.extra) == b.keys() - a.keys()
    assert set(rep.different) == {k for k in a.keys() & b.keys() if a[k] != b[k]}


@settings(max_examples=200)
@given(rows, st.lists(st.tuples(st.integers(0, 60).map(lambda i: (i,)), st.none() | st.text(max_size=3)), max_size=30), st.integers(1, 3), st.integers(1, 7))
def test_a_target_that_is_only_behind_is_equal(r, changes, speed, n):
    final = dict(r)
    for k, v in changes:
        if v is None:
            final.pop(k, None)
        else:
            final[k] = v
    s = Memory(final, pos=len(changes))
    rep = run(s, Behind(r, changes, speed), rows=n)
    assert rep.ok, rep


def test_a_range_that_never_settles_is_reported():
    s = Memory({(1,): "a"})
    count = iter(range(10**9))

    def churn():
        s.rows[(1,)] = str(next(count))

    rep = run(s, Behind({(1,): "a"}, [], 1, churn), rounds=8)
    assert not rep.ok and rep.not_settled == [(None, None)] and not rep.different


def test_a_busy_range_settles_when_no_row_is_wrong():
    # Another row is behind every time, never the same one for long.
    s = Memory({(i,): "a" for i in range(20)}, pos=10**6)
    t = Behind(dict(s.rows), [], 1)
    turn = iter(range(10**9))

    def churn():
        t.rows = dict(s.rows)
        t.rows[(next(turn) % 20,)] = "older"

    t.churn, t.pos = churn, 10**6
    rep = run(s, t)
    assert rep.ok and rep.compared_again > 0


def test_a_target_that_stops_is_reported():
    s = Memory({(1,): "b"}, pos=10)
    rep = run(s, Memory({(1,): "a"}, pos=3), rounds=4)
    assert rep.not_settled == [(None, None)]


def test_a_revoked_grant_that_came_back_is_found_under_load():
    s = Memory({(i,): "active" for i in range(50)}, pos=100)
    t = Behind({(i,): "active" for i in range(51)}, [((i,), "active") for i in range(50)] * 2, 5)
    assert run(s, t, rows=10).extra == [(50,)]
