import { clsx, type ClassValue } from "clsx";
import { twMerge } from "tailwind-merge";
import type { Timestamp } from "./types";

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs));
}

export function shortId(value?: string, size = 9) {
  return value ? value.slice(0, size) : "—";
}

export function formatDuration(milliseconds?: number) {
  if (milliseconds == null) return "Not started";
  const seconds = Math.max(0, Math.round(milliseconds / 1_000));
  if (seconds < 60) return `${seconds}s`;
  const minutes = Math.floor(seconds / 60);
  const remainder = seconds % 60;
  if (minutes < 60) return `${minutes}m ${remainder}s`;
  return `${Math.floor(minutes / 60)}h ${minutes % 60}m`;
}

export function formatBytes(value: number) {
  if (!value) return "0 B";
  const units = ["B", "KiB", "MiB", "GiB", "TiB"];
  const exponent = Math.min(Math.floor(Math.log(value) / Math.log(1024)), units.length - 1);
  return `${(value / 1024 ** exponent).toFixed(exponent > 1 ? 1 : 0)} ${units[exponent]}`;
}

/**
 * Milliseconds since the epoch for a service {@link Timestamp}. This is the one place the UI
 * decodes the wire tuple; every timestamp display goes through it.
 */
export function timestampMs(value: Timestamp): number {
  const [year, ordinal, hour, minute, second, nanosecond, offsetHours, offsetMinutes, offsetSeconds] = value;
  const offset = offsetHours * 3_600 + offsetMinutes * 60 + offsetSeconds;
  return Date.UTC(year, 0, ordinal, hour, minute, second, Math.floor(nanosecond / 1_000_000)) - offset * 1_000;
}

/** How long before `now` a service {@link Timestamp} was, as a short phrase. */
export function relativeTime(value: Timestamp, now = Date.now()) {
  const delta = now - timestampMs(value);
  const minutes = Math.round(delta / 60_000);
  if (minutes < 1) return "just now";
  if (minutes < 60) return `${minutes}m ago`;
  const hours = Math.round(minutes / 60);
  if (hours < 24) return `${hours}h ago`;
  return `${Math.round(hours / 24)}d ago`;
}

export function isTauri() {
  return "__TAURI_INTERNALS__" in window;
}

