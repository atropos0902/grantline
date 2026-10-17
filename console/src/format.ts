import type { Duration, Timestamp } from "@bufbuild/protobuf/wkt";

export function ms(d: Duration | undefined): number | undefined {
  return d && Number(d.seconds) * 1000 + d.nanos / 1e6;
}

export function lagText(v: number | undefined): string {
  if (v === undefined) return "no heartbeat yet";
  if (v < 1000) return `${Math.round(v)} ms`;
  if (v < 120_000) return `${(v / 1000).toFixed(1)} s`;
  return `${Math.round(v / 60_000)} min`;
}

export function bytes(n: bigint | number): string {
  const units = ["B", "KiB", "MiB", "GiB", "TiB"];
  let v = Number(n);
  let i = 0;
  while (v >= 1024 && i < units.length - 1) {
    v /= 1024;
    i++;
  }
  return `${i === 0 ? v : v.toFixed(1)} ${units[i]}`;
}

export function count(n: bigint | number): string {
  return Number(n).toLocaleString("en-US");
}

export function time(t: Timestamp | undefined): string {
  if (!t) return "";
  return new Date(Number(t.seconds) * 1000 + t.nanos / 1e6).toISOString().slice(11, 23);
}

/** How a chunk is drawn. */
export function chunkState(c: { landed: boolean; refused: bigint }): "waiting" | "landed" | "refused" {
  if (!c.landed) return "waiting";
  return c.refused > 0n ? "refused" : "landed";
}

/** A line through the values, as an SVG path in a w by h box. */
export function line(values: number[], w: number, h: number, top: number): string {
  if (values.length < 2) return "";
  return values
    .map((v, i) => {
      const x = (i / (values.length - 1)) * w;
      const y = h - (Math.min(v, top) / top) * h;
      return `${i ? "L" : "M"}${x.toFixed(1)} ${y.toFixed(1)}`;
    })
    .join("");
}
