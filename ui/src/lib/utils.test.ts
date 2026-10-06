import { expect, test } from "vitest";
import { relativeTime, timestampMs } from "./utils";
import type { Timestamp } from "./types";

// Payloads in the service's wire format: `[year, ordinal day, hour, minute, second, nanosecond, offset h, m, s]`.
const utc: Timestamp = [2026, 227, 11, 13, 0, 0, 0, 0, 0];
const expected = Date.UTC(2026, 7, 15, 11, 13, 0);

test("timestampMs decodes the wire tuple at any offset", () => {
  expect(timestampMs(utc)).toBe(expected);
  expect(timestampMs([2026, 227, 13, 13, 0, 0, 2, 0, 0])).toBe(expected);
  expect(timestampMs([2026, 227, 5, 43, 0, 0, -5, -30, 0])).toBe(expected);
  expect(timestampMs([2026, 227, 11, 13, 7, 250_999_999, 0, 0, 0])).toBe(expected + 7_250);
  expect(timestampMs([2024, 366, 23, 59, 59, 0, 0, 0, 0])).toBe(Date.UTC(2024, 11, 31, 23, 59, 59));
});

test("relativeTime measures age from the wire tuple", () => {
  expect(relativeTime(utc, expected + 20_000)).toBe("just now");
  expect(relativeTime(utc, expected + 47 * 60_000)).toBe("47m ago");
  expect(relativeTime([2026, 227, 13, 13, 0, 0, 2, 0, 0], expected + 3 * 3_600_000)).toBe("3h ago");
  expect(relativeTime(utc, expected + 2 * 86_400_000)).toBe("2d ago");
});
