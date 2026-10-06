import { RefreshCw } from "lucide-react";
import type { RepositorySnapshot } from "../lib/types";
import type { Route } from "./useAppState";
import { Badge } from "../components/ui/Badge";
import { Button } from "../components/ui/Button";
import { repositoryStatus } from "../components/StatusGlyph";
import { oidHex } from "../lib/types";
import { shortId } from "../lib/utils";
import { releaseLagLabel } from "../features/release/release";

const titles: Record<Route, string> = { runs: "Gate", checks: "Checks", release: "Release" };

export function Topbar({ repository, route, onRefresh, refreshing }: {
  repository?: RepositorySnapshot;
  route: Route;
  onRefresh: () => void;
  refreshing: boolean;
}) {
  const status = repository && repositoryStatus(repository.state.execution_state);
  return <header className="topbar" data-tauri-drag-region>
    <div className="topbar__title" data-tauri-drag-region>
      <strong>{titles[route]}</strong>
      {status && <Badge tone={status.tone} dot>{status.label}</Badge>}
    </div>
    {repository && <dl className="topbar__refs" aria-label="Tollgate refs">
      <div><dt>staging</dt><dd><code>{shortId(oidHex(repository.state.staging_oid), 7)}</code></dd></div>
      <div><dt>release</dt><dd><code>{shortId(oidHex(repository.state.release_oid), 7)}</code></dd></div>
      <div><dt>lag</dt><dd>{releaseLagLabel(repository.state.release_lag)}</dd></div>
    </dl>}
    {repository && <div className="topbar__actions">
      <Button size="icon" variant="ghost" onClick={onRefresh} aria-label="Refresh"><RefreshCw className={refreshing ? "spin" : ""} /></Button>
    </div>}
  </header>;
}
