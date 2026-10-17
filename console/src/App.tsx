import { createClient } from "@connectrpc/connect";
import { TransportProvider, useMutation, useQuery } from "@connectrpc/connect-query";
import { createConnectTransport } from "@connectrpc/connect-web";
import { useQuery as useFetch } from "@tanstack/react-query";
import { type FormEvent, useEffect, useMemo, useState } from "react";
import { approveSchemaChange, explainKey, listChunks, pause, resume } from "./gen/grantline/v1/control-ControlService_connectquery";
import { ControlService, Phase, type Status } from "./gen/grantline/v1/control_pb";
import { bytes, chunkState, count, lagText, line, ms, time } from "./format";

export function App() {
  const names = useFetch({
    queryKey: ["replications"],
    queryFn: async (): Promise<string[]> => (await fetch("/api/replications")).json(),
  });
  const [picked, pick] = useState<string>();
  const name = picked ?? names.data?.[0];
  return (
    <main>
      <header>
        <h1>Grantline</h1>
        <nav aria-label="Replications">
          {names.data?.map((n) => (
            <button key={n} type="button" aria-pressed={n === name} onClick={() => pick(n)}>
              {n}
            </button>
          ))}
        </nav>
      </header>
      {names.error && <p className="bad">The gateway doesn't answer.</p>}
      {name && <Replication key={name} name={name} />}
      <Events />
    </main>
  );
}

function Replication({ name }: { name: string }) {
  const transport = useMemo(() => createConnectTransport({ baseUrl: `/w/${name}` }), [name]);
  const [status, setStatus] = useState<Status>();
  const [lags, setLags] = useState<number[]>([]);
  const [lost, setLost] = useState(false);

  useEffect(() => {
    const stop = new AbortController();
    (async () => {
      const client = createClient(ControlService, transport);
      while (!stop.signal.aborted) {
        try {
          for await (const m of client.watchStatus({}, { signal: stop.signal })) {
            setLost(false);
            setStatus(m.status);
            const lag = ms(m.status?.lag);
            if (lag !== undefined) setLags((l) => [...l.slice(-119), lag]);
          }
        } catch {
          setLost(true);
        }
        await new Promise((r) => setTimeout(r, 1000));
      }
    })();
    return () => stop.abort();
  }, [transport]);

  return (
    <TransportProvider transport={transport}>
      {lost && <p className="bad">The worker doesn't answer. Numbers below are the last ones it sent.</p>}
      {status && (
        <>
          <Numbers status={status} lags={lags} />
          <Chunks copying={status.phase === Phase.COPYING} />
          <Explain />
        </>
      )}
    </TransportProvider>
  );
}

const phaseName: Record<Phase, string> = {
  [Phase.UNSPECIFIED]: "not running",
  [Phase.NEW]: "starting",
  [Phase.COPYING]: "copying, don't read it yet",
  [Phase.READY]: "ready",
};

function Numbers({ status: s, lags }: { status: Status; lags: number[] }) {
  const doPause = useMutation(pause);
  const doResume = useMutation(resume);
  const approve = useMutation(approveSchemaChange);
  const lag = ms(s.lag);
  const top = Math.max(1000, ...lags);
  return (
    <section>
      <div className="lag">
        <div>
          <span className={lag !== undefined && lag < 1000 ? "big" : "big bad"}>{lagText(lag)}</span>
          <span className="note">behind the source</span>
        </div>
        <svg viewBox="0 0 240 48" preserveAspectRatio="none" role="img" aria-label={`Lag over the last two minutes, up to ${lagText(top)}`}>
          <path d={`M0 ${48 - (1000 / top) * 48}H240`} className="limit" />
          <path d={line(lags, 240, 48, top)} className="line" />
        </svg>
      </div>
      {s.failure && <p className="bad">Stopped: {s.failure}</p>}
      {s.waitingStatement && (
        <div className="ask">
          <p>The source changed shape. The replication is paused until this runs on the target:</p>
          <pre>{s.waitingStatement}</pre>
          <button type="button" onClick={() => approve.mutate({ statement: s.waitingStatement })}>
            Run it
          </button>
          {approve.error && <p className="bad">{approve.error.rawMessage}</p>}
        </div>
      )}
      <dl>
        <Item name="State" value={s.paused ? "paused" : phaseName[s.phase]} />
        <Item name="Applied" value={s.appliedLsn} />
        <Item name="Source" value={s.sourceLsn} />
        <Item name="Log kept at the source" value={bytes(s.retainedLogBytes)} />
        <Item name="Changes" value={count(s.changes)} />
        <Item name="Rows copied" value={count(s.rowsCopied)} />
        <Item name="Older rows left out" value={count(s.rowsRefused)} />
        <Item name="Keys read again" value={count(s.keysReread)} />
        <Item name="Locks given up" value={count(s.yields)} />
        <Item name="Epoch" value={count(s.epoch)} />
      </dl>
      <button type="button" onClick={() => (s.paused ? doResume : doPause).mutate({})}>
        {s.paused ? "Resume" : "Pause"}
      </button>
    </section>
  );
}

function Item({ name, value }: { name: string; value: string }) {
  return (
    <div>
      <dt>{name}</dt>
      <dd>{value}</dd>
    </div>
  );
}

function Chunks({ copying }: { copying: boolean }) {
  const q = useQuery(listChunks, { table: "" }, { refetchInterval: copying ? 1000 : 15_000, retry: false });
  const tables = useMemo(() => Map.groupBy(q.data?.chunks ?? [], (c) => c.table), [q.data]);
  // A Kafka target has no chunks, and says so with an error.
  if (!q.data?.chunks.length) return null;
  return (
    <section>
      <h2>Chunks</h2>
      <p className="note">
        <i className="key landed" /> landed <i className="key refused" /> landed, with older rows left out <i className="key waiting" /> waiting
      </p>
      {[...tables].map(([table, chunks]) => (
        <div key={table}>
          <h3>
            {table} <span className="note">{chunks.length} chunks</span>
          </h3>
          <div className="map">
            {chunks.map((c) => (
              <i
                key={c.number}
                className={chunkState(c)}
                title={`chunk ${c.number}: ${c.lo.join(", ") || "start"} to ${c.hi.join(", ") || "end"}. ${count(c.rows)} rows, ${count(c.refused)} left out, snapshot ${c.snapshot || "none yet"}`}
              />
            ))}
          </div>
        </div>
      ))}
    </section>
  );
}

function Explain() {
  const [form, setForm] = useState({ table: "grants", key: "" });
  const [asked, ask] = useState<{ table: string; key: string[] }>();
  const q = useQuery(explainKey, asked ?? { table: "", key: [] }, { enabled: asked !== undefined, retry: false });
  const submit = (e: FormEvent) => {
    e.preventDefault();
    const key = form.key.split(",").map((s) => s.trim());
    ask({ table: form.table, key });
    if (asked) void q.refetch();
  };
  return (
    <section>
      <h2>What happened to a key</h2>
      <form onSubmit={submit}>
        <label>
          Table <input value={form.table} onChange={(e) => setForm({ ...form, table: e.target.value })} required />
        </label>
        <label>
          Key <input value={form.key} onChange={(e) => setForm({ ...form, key: e.target.value })} placeholder="8814, or a comma between parts" required />
        </label>
        <button type="submit">Explain</button>
      </form>
      {q.error && asked && <p className="bad">{q.error.rawMessage}</p>}
      {q.data && (
        <>
          <table>
            <thead>
              <tr>
                <th>At</th>
                <th>What</th>
                <th>Transaction</th>
                <th>Position</th>
                <th>Why</th>
              </tr>
            </thead>
            <tbody>
              {q.data.steps.map((s, i) => (
                <tr key={i} className={s.what}>
                  <td>{time(s.at)}</td>
                  <td>{s.what}</td>
                  <td>{s.xid ? String(s.xid) : ""}</td>
                  <td>{s.lsn}</td>
                  <td>{s.reason}</td>
                </tr>
              ))}
            </tbody>
          </table>
          {q.data.steps.length === 0 && <p className="note">Nothing is recorded for this key.</p>}
          <dl className="rows">
            <Item name="Source now" value={q.data.sourceRow || "no row"} />
            <Item name="Target now" value={q.data.targetRow || "no row"} />
          </dl>
        </>
      )}
    </section>
  );
}

interface Event {
  replication: string;
  kind: string;
  at: string;
  fields?: Record<string, unknown>;
}

function Events() {
  const [events, setEvents] = useState<Event[]>([]);
  useEffect(() => {
    const es = new EventSource("/api/events");
    es.onmessage = (m) => setEvents((l) => [JSON.parse(m.data) as Event, ...l].slice(0, 100));
    return () => es.close();
  }, []);
  if (events.length === 0) return null;
  return (
    <section>
      <h2>Events</h2>
      <ol className="events">
        {events.map((e, i) => (
          <li key={i}>
            <time>{e.at.slice(11, 19)}</time> <b>{e.replication}</b> {e.kind.replaceAll("_", " ")}{" "}
            <span className="note">
              {Object.entries(e.fields ?? {})
                .map(([k, v]) => `${k} ${String(v)}`)
                .join(", ")}
            </span>
          </li>
        ))}
      </ol>
    </section>
  );
}
