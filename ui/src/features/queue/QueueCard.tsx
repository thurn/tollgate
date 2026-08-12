import { motion } from "framer-motion";
import { ArrowRight, Box, CircleDotDashed, Clock3, GitBranch, GitCommitHorizontal, Layers3, Network, ServerCog, ShieldCheck } from "lucide-react";
import type { QueueItemView } from "../../lib/types";
import { oidHex } from "../../lib/types";
import { cn, formatDuration, shortId } from "../../lib/utils";
import { StatusGlyph, itemStatus } from "../../components/StatusGlyph";
import { Badge } from "../../components/ui/Badge";
import { Tooltip } from "../../components/ui/Tooltip";

export function QueueCard({ view, position, selected, eligible, onSelect }: { view: QueueItemView; position: number; selected: boolean; eligible: boolean; onSelect: () => void }) {
  const { item, generation, buildset } = view;
  const status = itemStatus(item.state);
  const branch = item.metadata.branch ?? "detached source";
  const warning = buildset?.state === "passed-with-warnings" || Boolean(view.certificate?.warnings.length);
  return <motion.article layout="position" className={cn("queue-card", selected && "is-selected", `queue-card--${status.tone}`)} aria-label={`Queue item ${position}: ${item.metadata.subject}, ${status.label}`} initial={{ opacity: 0, y: 10 }} animate={{ opacity: 1, y: 0 }} exit={{ opacity: 0, height: 0, marginBottom: 0 }} transition={{ type: "spring", stiffness: 420, damping: 38 }}>
    <div className="queue-card__rail">
      <span className="queue-card__position">{position}</span>
      <StatusGlyph state={item.state} />
      <span className="queue-card__line" />
    </div>
    <div className="queue-card__body">
      <div className="queue-card__header">
        <div className="queue-card__identity">
          <div className="queue-card__title-line"><h3><button className="queue-card__disclosure" onClick={onSelect} aria-expanded={selected} aria-controls="item-inspector">{item.metadata.subject}</button></h3>{item.dependencies.length > 0 && <Tooltip label="This change has a hard Git dependency"><span className="dependency-mark"><Network size={13} />stacked</span></Tooltip>}</div>
          <div className="queue-card__meta"><span><GitBranch size={13} />{branch}</span><span><GitCommitHorizontal size={13} /><code>{shortId(oidHex(item.source_oid), 8)}</code></span><span>{item.metadata.author_name}</span><span>{item.promotion_authorized ? "approved" : "submitted"} {formatDuration(Date.now() - new Date(item.metadata.approved_at).getTime())} ago</span></div>
        </div>
        <div className="queue-card__state"><Badge tone={status.tone} dot={item.state === "running"}>{status.label}</Badge>{!item.promotion_authorized && <Badge tone="neutral">awaiting approval</Badge>}{warning && <Badge tone="warning">warnings</Badge>}{item.remote_state === "push-blocked" && <Badge tone="danger">push blocked</Badge>}{item.cleanup_state === "needs-attention" && <Badge tone="warning">cleanup</Badge>}</div>
      </div>
      <div className="prefix-strip">
        <Tooltip label={`Anchored base ${oidHex(generation?.anchored_base_oid)}`}><span><Box size={13} /><small>base</small><code>{shortId(oidHex(generation?.anchored_base_oid), 7)}</code></span></Tooltip>
        <ArrowRight size={13} />
        <span className="prefix-strip__composition"><Layers3 size={13} /><small>prefix</small><strong>{view.included_items.length || position} patch{(view.included_items.length || position) === 1 ? "" : "es"}</strong></span>
        <ArrowRight size={13} />
        <Tooltip label={`Exact tested commit ${oidHex(generation?.tested_oid)}`}><span className="prefix-strip__tested"><ShieldCheck size={13} /><small>tested</small><code>{shortId(oidHex(generation?.tested_oid), 7)}</code></span></Tooltip>
        <span className="prefix-strip__generation"><CircleDotDashed size={12} />gen {shortId(generation?.identity_digest, 6)}</span>
      </div>
      <div className="queue-card__footer">
        <div className="step-summary">
          <span className={cn("step-dot", ["ready"].includes(item.state) && "is-success", item.state === "running" && "is-running")} />
          <strong>{item.state === "ready" ? warning ? "Certificate ready with non-voting warnings" : "Pass certificate ready" : item.state === "running" ? "Validation in progress" : item.state === "queued" ? "Waiting for capacity" : status.label}</strong>
          {buildset?.slot_id && <span><ServerCog size={13} />slot {shortId(buildset.slot_id, 4)}</span>}
        </div>
        <div className="queue-card__timing"><Clock3 size={13} /><span>{formatDuration(view.elapsed_ms)}</span></div>
      </div>
      {!eligible && <div className="window-note">Outside active window</div>}
    </div>
  </motion.article>;
}
