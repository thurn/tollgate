import { useEffect, useMemo, useState } from "react";
import { useInfiniteQuery, useQuery, useQueryClient } from "@tanstack/react-query";
import { listen } from "@tauri-apps/api/event";
import { getHistoryItems, getItemDetails, getSnapshot } from "../lib/api";
import { isTauri } from "../lib/utils";

export type Route = "runs" | "checks";

function routeFromStorage(value: unknown): Route {
  return value === "checks" ? "checks" : "runs";
}

export function useAppState() {
  const queryClient = useQueryClient();
  const query = useQuery({ queryKey: ["snapshot"], queryFn: getSnapshot, refetchInterval: isTauri() ? 2_000 : false });
  const [repositoryId, setRepositoryId] = useState(() => localStorage.getItem("tollgate.repository"));
  const [route, setRouteState] = useState<Route>(() => routeFromStorage(localStorage.getItem("tollgate.route")));
  const [selectedItemId, setSelectedItemId] = useState<string | null>(() => localStorage.getItem("tollgate.item"));

  useEffect(() => {
    if (!isTauri()) return;
    let stop: undefined | (() => void);
    void listen("tollgate://snapshot-changed", () => {
      void queryClient.invalidateQueries({ queryKey: ["snapshot"] });
      void queryClient.invalidateQueries({ queryKey: ["history-items"] });
    }).then((unlisten) => { stop = unlisten; });
    return () => stop?.();
  }, [queryClient]);

  useEffect(() => {
    const current = { repositoryId, route, selectedItemId };
    if (!window.history.state?.tollgate) window.history.replaceState({ tollgate: current }, "");
    const pop = (event: PopStateEvent) => {
      const value = event.state?.tollgate as typeof current | undefined;
      if (!value) return;
      setRepositoryId(value.repositoryId);
      setRouteState(routeFromStorage(value.route));
      setSelectedItemId(value.selectedItemId);
    };
    window.addEventListener("popstate", pop);
    return () => window.removeEventListener("popstate", pop);
  }, [repositoryId, route, selectedItemId]);

  useEffect(() => {
    const first = query.data?.repositories[0]?.state.id;
    if (first && !query.data?.repositories.some((repository) => repository.state.id === repositoryId)) setRepositoryId(first);
  }, [query.data, repositoryId]);

  const repository = useMemo(() => query.data?.repositories.find((candidate) => candidate.state.id === repositoryId) ?? query.data?.repositories[0], [query.data, repositoryId]);
  const historyQuery = useInfiniteQuery({
    queryKey: ["history-items", repository?.state.id],
    queryFn: ({ pageParam }) => getHistoryItems(repository!.state.id, pageParam, 50),
    initialPageParam: 0,
    enabled: !!repository,
    getNextPageParam: (page) => page.offset + page.items.length < page.total ? page.offset + page.items.length : undefined,
  });
  const historyItems = historyQuery.data?.pages.flatMap((page) => page.items) ?? repository?.history_items ?? [];
  const snapshotItem = repository
    ? [...repository.queue, ...repository.checks, ...historyItems].find((candidate) => candidate.item.id === selectedItemId) ?? null
    : null;
  const detailQuery = useQuery({ queryKey: ["item-details", repository?.state.id, selectedItemId], queryFn: () => getItemDetails(repository!.state.id, selectedItemId!), enabled: isTauri() && !!repository && !!selectedItemId && !snapshotItem, retry: false });
  const selectedItem = snapshotItem ?? detailQuery.data ?? null;

  function remember(next: { repositoryId: string | null; route: Route; selectedItemId: string | null }) { window.history.pushState({ tollgate: next }, ""); }
  function selectRepository(id: string) { setRepositoryId(id); localStorage.setItem("tollgate.repository", id); setSelectedItemId(null); remember({ repositoryId: id, route, selectedItemId: null }); }
  function setRoute(value: Route) { setRouteState(value); localStorage.setItem("tollgate.route", value); remember({ repositoryId, route: value, selectedItemId }); }
  function selectItem(id: string | null) { setSelectedItemId(id); if (id) localStorage.setItem("tollgate.item", id); else localStorage.removeItem("tollgate.item"); remember({ repositoryId, route, selectedItemId: id }); }
  return {
    ...query,
    snapshot: query.data,
    repository,
    selectedItem,
    historyItems,
    hasMoreHistory: historyQuery.hasNextPage,
    isLoadingMoreHistory: historyQuery.isFetchingNextPage,
    loadMoreHistory: historyQuery.fetchNextPage,
    repositoryId,
    route,
    selectRepository,
    setRoute,
    selectItem,
  };
}
