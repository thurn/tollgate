import { Clock3, GitMerge } from "lucide-react";
import type { AppSnapshot } from "../lib/types";
import type { Route } from "./useAppState";
import { cn } from "../lib/utils";

const navigation: { route: Route; label: string; icon: typeof GitMerge }[] = [
  { route: "queue", label: "Gate", icon: GitMerge },
  { route: "history", label: "History", icon: Clock3 },
];

export function Sidebar({ snapshot, selectedRepository, route, onRepository, onRoute }: {
  snapshot?: AppSnapshot;
  selectedRepository?: string | null;
  route: Route;
  onRepository: (id: string) => void;
  onRoute: (route: Route) => void;
}) {
  return <aside className="sidebar">
    <div className="sidebar__drag" data-tauri-drag-region />
    <strong className="brand">Tollgate</strong>
    <div className="repository-list">
      {snapshot?.repositories.map((repository) => <button
        key={repository.state.id}
        className={cn("repository-button", selectedRepository === repository.state.id && "is-active")}
        onClick={() => onRepository(repository.state.id)}
      >
        <span className={cn("repository-dot", `tone-${repository.state.execution_state === "active" ? "success" : repository.state.execution_state === "blocked" ? "danger" : "warning"}`)} />
        <span>{repository.state.name}</span>
        {repository.queue.length > 0 && <small>{repository.queue.length}</small>}
      </button>)}
    </div>
    <nav className="sidebar__nav" aria-label="Repository navigation">
      {navigation.map(({ route: value, label, icon: Icon }) => <button key={value} className={cn(route === value && "is-active")} onClick={() => onRoute(value)} aria-current={route === value ? "page" : undefined}><Icon />{label}</button>)}
    </nav>
  </aside>;
}
