import { RefreshCw } from "lucide-react";
import type { RepositorySnapshot } from "../lib/types";
import type { Route } from "./useAppState";
import { Badge } from "../components/ui/Badge";
import { Button } from "../components/ui/Button";
import { repositoryStatus } from "../components/StatusGlyph";

export function Topbar({ repository, route, onRefresh, refreshing }: {
  repository?: RepositorySnapshot;
  route: Route;
  onRefresh: () => void;
  refreshing: boolean;
}) {
  const status = repository && repositoryStatus(repository.state.execution_state);
  return <header className="topbar" data-tauri-drag-region>
    <div className="topbar__title" data-tauri-drag-region>
      <strong>{route === "runs" ? "Gate" : "Checks"}</strong>
      {status && <Badge tone={status.tone} dot>{status.label}</Badge>}
    </div>
    {repository && <div className="topbar__actions">
      <Button size="icon" variant="ghost" onClick={onRefresh} aria-label="Refresh"><RefreshCw className={refreshing ? "spin" : ""} /></Button>
    </div>}
  </header>;
}
