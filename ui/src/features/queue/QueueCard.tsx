import { ChevronRight, GitBranch } from "lucide-react";
import type { QueueItemView } from "../../lib/types";
import { oidHex } from "../../lib/types";
import { cn, formatDuration, shortId } from "../../lib/utils";
import { StatusGlyph, itemStatus } from "../../components/StatusGlyph";

export function QueueCard({ view, position, selected, onSelect }: { view: QueueItemView; position: number; selected: boolean; onSelect: () => void }) {
  const status = itemStatus(view.item.state);
  return <article className={cn("queue-card", selected && "is-selected")}>
    <span className="queue-card__position">{position}</span>
    <StatusGlyph state={view.item.state} />
    <button className="queue-card__main" onClick={onSelect} aria-expanded={selected} aria-controls="item-inspector">
      <strong>{view.item.metadata.subject}</strong>
      <span><GitBranch />{view.item.metadata.branch ?? "detached"} · <code>{shortId(oidHex(view.item.source_oid), 8)}</code></span>
    </button>
    <span className={cn("queue-card__status", `tone-${status.tone}`)}>{status.label}</span>
    <time>{view.elapsed_ms ? formatDuration(view.elapsed_ms) : "—"}</time>
    <ChevronRight className="queue-card__chevron" />
  </article>;
}
