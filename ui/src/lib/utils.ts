import { clsx, type ClassValue } from "clsx";
import { twMerge } from "tailwind-merge";

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

/** Milliseconds since the epoch for a serialized Rust `OffsetDateTime`, or null when unreadable. */
export function timestampMs(value: unknown): number | null {
  if (typeof value === "string") {
    const parsed = Date.parse(value);
    return Number.isNaN(parsed) ? null : parsed;
  }
  if (Array.isArray(value) && value.length >= 6 && value.every((part) => typeof part === "number")) {
    const [year = 0, ordinal = 1, hour = 0, minute = 0, second = 0, nanosecond = 0, offsetHours = 0, offsetMinutes = 0, offsetSeconds = 0] = value as number[];
    const offset = offsetHours * 3_600 + offsetMinutes * 60 + offsetSeconds;
    return Date.UTC(year, 0, ordinal, hour, minute, second) + Math.floor(nanosecond / 1_000_000) - offset * 1_000;
  }
  return null;
}

export function relativeTime(value: string) {
  const delta = Date.now() - new Date(value).getTime();
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

