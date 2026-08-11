import { AnimatePresence, motion } from "framer-motion";
import { Activity, AlertTriangle, Boxes, ChevronsLeft, ChevronsRight, CircleGauge, Clock3, Database, FileCode2, GitMerge, HeartPulse, Plus, Settings2, Trash2 } from "lucide-react";
import type { AppSnapshot } from "../lib/types";
import type { Route } from "./useAppState";
import { cn } from "../lib/utils";
import { Tooltip } from "../components/ui/Tooltip";
import { repositoryStatus } from "../components/StatusGlyph";

const navigation: { route: Route; label: string; icon: typeof Activity }[] = [
  { route: "queue", label: "Gate", icon: GitMerge },
  { route: "history", label: "History", icon: Clock3 },
  { route: "resources", label: "Resources", icon: CircleGauge },
  { route: "storage", label: "Storage", icon: Database },
  { route: "configuration", label: "Configuration", icon: FileCode2 },
  { route: "doctor", label: "Doctor", icon: HeartPulse },
];

export function Sidebar({ snapshot, selectedRepository, route, collapsed, onRepository, onRoute, onToggle, onAdd, onRemove, onPreferences }: { snapshot?: AppSnapshot; selectedRepository?: string | null; route: Route; collapsed: boolean; onRepository: (id: string) => void; onRoute: (route: Route) => void; onToggle: () => void; onAdd: () => void; onRemove: (id: string) => void; onPreferences: () => void }) {
  return (
    <motion.aside className="sidebar" animate={{ width: collapsed ? 76 : 252 }} transition={{ type: "spring", stiffness: 430, damping: 40 }} data-collapsed={collapsed}>
      <div className="sidebar__traffic-space" data-tauri-drag-region />
      <div className="brand" aria-label="Tollgate">
        <div className="brand__mark"><span /><span /><span /></div>
        <AnimatePresence initial={false}>{!collapsed && <motion.div className="brand__word" initial={{ opacity: 0, x: -5 }} animate={{ opacity: 1, x: 0 }} exit={{ opacity: 0, x: -5 }}><strong>Tollgate</strong><small>LOCAL GATE</small></motion.div>}</AnimatePresence>
      </div>
      <div className="sidebar__section-label">{collapsed ? <Boxes size={14} /> : "Repositories"}</div>
      <div className="repository-list">
        {snapshot?.repositories.map((repository) => {
          const status = repositoryStatus(repository.state.execution_state);
          const running = repository.queue.filter((item) => item.item.state === "running").length;
          const failures = repository.history_items.filter((item) => ["failed", "infrastructure-exhausted", "merge-conflict", "dependency-failed", "check-failed"].includes(item.item.state)).length;
          const content = <button key={repository.state.id} className={cn("repository-button", selectedRepository === repository.state.id && "is-active")} onClick={() => onRepository(repository.state.id)} aria-label={`${repository.state.name}, ${status.label}`}>
            <span className={cn("repository-button__avatar", `tone-${status.tone}`)}>{repository.state.name.slice(0, 2).toUpperCase()}</span>
            {!collapsed && <><span className="repository-button__copy"><strong>{repository.state.name}</strong><small>{running ? `${running} running` : status.label}</small></span><span className="repository-button__badges">{failures > 0 && <b className="count count--danger">{failures}</b>}{repository.queue.length > 0 && <b className="count">{repository.queue.length}</b>}</span></>}
          </button>;
          return collapsed ? <Tooltip key={repository.state.id} label={repository.state.name} side="right">{content}</Tooltip> : <div className="repository-row" key={repository.state.id}>{content}<button className="repository-row__remove" onClick={() => onRemove(repository.state.id)} aria-label={`Remove ${repository.state.name} from Tollgate`} title="Remove from Tollgate"><Trash2 size={12} /></button></div>;
        })}
        {snapshot?.unavailable_repositories.map((repository) => {
          const content = <button key={repository.id} className="repository-button is-unavailable" disabled aria-label={`${repository.name}, unavailable: ${repository.error}`} title={`${repository.error}\n\n${repository.recovery_action}`}><span className="repository-button__avatar tone-danger"><AlertTriangle size={14} /></span>{!collapsed && <><span className="repository-button__copy"><strong>{repository.name}</strong><small>Unavailable · repair required</small></span><span className="repository-button__badges"><b className="count count--danger">!</b></span></>}</button>;
          return collapsed ? <Tooltip key={repository.id} label={`${repository.name} · unavailable`} side="right">{content}</Tooltip> : <div className="repository-row" key={repository.id}>{content}<button className="repository-row__remove" onClick={() => onRemove(repository.id)} aria-label={`Remove unavailable ${repository.name} from Tollgate`} title="Remove from Tollgate"><Trash2 size={12} /></button></div>;
        })}
        <Tooltip label="Open or initialize a repository" side="right"><button className="repository-button repository-button--add" onClick={onAdd}><span className="repository-button__add"><Plus size={16} /></span>{!collapsed && <span>Add repository</span>}</button></Tooltip>
      </div>
      <div className="sidebar__separator" />
      <nav className="sidebar__nav" aria-label="Repository navigation">
        {navigation.map(({ route: value, label, icon: Icon }) => {
          const button = <button key={value} className={cn(route === value && "is-active")} onClick={() => onRoute(value)} aria-current={route === value ? "page" : undefined}><Icon size={17} />{!collapsed && <span>{label}</span>}{route === value && <motion.i layoutId="sidebar-nav-active" />}</button>;
          return collapsed ? <Tooltip key={value} label={label} side="right">{button}</Tooltip> : button;
        })}
      </nav>
      <div className="sidebar__bottom">
        <button className="sidebar__settings" onClick={onPreferences}><Settings2 size={17} />{!collapsed && <span>Preferences</span>}</button>
        <button className="sidebar__collapse" onClick={onToggle} aria-label={collapsed ? "Expand sidebar" : "Collapse sidebar"}>{collapsed ? <ChevronsRight size={16} /> : <><ChevronsLeft size={16} /><span>Collapse</span></>}</button>
      </div>
    </motion.aside>
  );
}
