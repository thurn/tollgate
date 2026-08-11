/* eslint-disable react-refresh/only-export-components -- pure appearance helpers are intentionally colocated */
import { AlertTriangle, Ban, Check, CircleDashed, Clock3, GitMerge, LoaderCircle, Pause, ShieldCheck, X } from "lucide-react";
import type { QueueItemState, RepositoryExecutionState } from "../lib/types";
import { cn } from "../lib/utils";

export function itemStatus(state: QueueItemState) {
  switch (state) {
    case "ready": return { label: "Validation passed", tone: "success" as const, icon: ShieldCheck };
    case "promoting": return { label: "Promoting", tone: "info" as const, icon: GitMerge };
    case "running": return { label: "Running", tone: "info" as const, icon: LoaderCircle };
    case "preparing": case "constructing": return { label: state === "preparing" ? "Preparing" : "Constructing", tone: "violet" as const, icon: CircleDashed };
    case "queued": return { label: "Queued", tone: "neutral" as const, icon: Clock3 };
    case "promoted": case "externally-integrated": return { label: state === "promoted" ? "Promoted" : "Externally integrated", tone: "success" as const, icon: Check };
    case "promoted-local-push-pending": return { label: "Push pending", tone: "warning" as const, icon: AlertTriangle };
    case "merge-conflict": return { label: "Merge conflict", tone: "danger" as const, icon: GitMerge };
    case "failed": case "infrastructure-exhausted": return { label: state === "failed" ? "Failed" : "Infrastructure exhausted", tone: "danger" as const, icon: X };
    case "check-passed": return { label: "Check passed", tone: "success" as const, icon: Check };
    case "check-failed": return { label: "Check failed", tone: "danger" as const, icon: X };
    case "canceled": case "superseded": case "dependency-failed": return { label: state.replaceAll("-", " "), tone: "neutral" as const, icon: Ban };
  }
}

export function StatusGlyph({ state, size = "md" }: { state: QueueItemState; size?: "sm" | "md" | "lg" }) {
  const status = itemStatus(state);
  const Icon = status.icon;
  return <span className={cn("status-glyph", `status-glyph--${status.tone}`, `status-glyph--${size}`, state === "running" && "status-glyph--pulse")} role="img" aria-label={status.label}><Icon aria-hidden /></span>;
}

export function repositoryStatus(state: RepositoryExecutionState) {
  switch (state) {
    case "active": return { label: "Gate active", tone: "success" as const };
    case "paused": return { label: "Gate paused", tone: "violet" as const, icon: Pause };
    case "configuration-pending": return { label: "Configuration pending", tone: "warning" as const };
    case "blocked": return { label: "Needs attention", tone: "danger" as const };
  }
}
