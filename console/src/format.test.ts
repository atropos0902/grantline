import { expect, test } from "vitest";
import { bytes, chunkState, lagText, line } from "./format";

test("lag reads in the unit that fits", () => {
  expect(lagText(undefined)).toBe("no heartbeat yet");
  expect(lagText(84.6)).toBe("85 ms");
  expect(lagText(2400)).toBe("2.4 s");
  expect(lagText(600_000)).toBe("10 min");
});

test("bytes", () => {
  expect(bytes(512n)).toBe("512 B");
  expect(bytes(3n * 1024n * 1024n)).toBe("3.0 MiB");
});

test("a chunk that left rows out stands apart from one that didn't", () => {
  expect(chunkState({ landed: false, refused: 0n })).toBe("waiting");
  expect(chunkState({ landed: true, refused: 0n })).toBe("landed");
  expect(chunkState({ landed: true, refused: 4n })).toBe("refused");
});

test("a line stays inside its box", () => {
  expect(line([0, 50, 500], 100, 20, 100)).toBe("M0.0 20.0L50.0 10.0L100.0 0.0");
  expect(line([1], 100, 20, 100)).toBe("");
});
