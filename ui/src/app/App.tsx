import { Sidebar } from "./Sidebar";
import { Topbar } from "./Topbar";
import { useAppState } from "./useAppState";
import { ItemInspector } from "../features/queue/ItemInspector";
import { RunsView } from "../features/runs/RunsView";
import { ChecksView } from "../features/checks/ChecksView";
import { EmptyState } from "./EmptyState";
import { Button } from "../components/ui/Button";

export function App() {
  const state = useAppState();

  if (state.isLoading) return <div className="loading" role="status">Loading Tollgate…</div>;
  if (state.isError) return <main className="error-state" role="alert"><h1>Tollgate could not load.</h1><p>{String(state.error)}</p><Button variant="primary" onClick={() => state.refetch()}>Retry</Button></main>;

  return <div className="app-shell">
    <Sidebar snapshot={state.snapshot} selectedRepository={state.repository?.state.id} route={state.route} onRepository={state.selectRepository} onRoute={state.setRoute} />
    <div className="workspace">
      <Topbar repository={state.repository} route={state.route} onRefresh={() => state.refetch()} refreshing={state.isFetching} />
      {!state.repository || !state.snapshot?.repositories.length ? <EmptyState /> : <div className="workspace__body">
        <main className="main-content">
          {state.route === "runs" && <RunsView repository={state.repository} historyItems={state.historyItems} selectedItemId={state.selectedItem?.item.id ?? null} onSelect={state.selectItem} hasMore={state.hasMoreHistory} loadingMore={state.isLoadingMoreHistory} onLoadMore={state.loadMoreHistory} />}
          {state.route === "checks" && <ChecksView repository={state.repository} historyItems={state.historyItems} selectedItemId={state.selectedItem?.item.id ?? null} onSelect={state.selectItem} hasMore={state.hasMoreHistory} loadingMore={state.isLoadingMoreHistory} onLoadMore={state.loadMoreHistory} />}
        </main>
        <ItemInspector view={state.selectedItem} repository={state.repository} onClose={() => state.selectItem(null)} />
      </div>}
    </div>
  </div>;
}
