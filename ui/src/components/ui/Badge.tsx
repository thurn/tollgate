import type { ReactNode } from "react";
import { cn } from "../../lib/utils";

export function Badge({ children, tone = "neutral", dot, className }: { children: ReactNode; tone?: "neutral" | "info" | "success" | "warning" | "danger" | "violet"; dot?: boolean; className?: string }) {
  return <span className={cn("badge", `badge--${tone}`, className)}>{dot && <span className="badge__dot" aria-hidden />}{children}</span>;
}

