import type { AppSnapshot, QueueItemView } from "../lib/types";

/** One repository's work that Quit would interrupt. */
export interface RepositoryActiveWork {
  repositoryId: string;
  repositoryName: string;
  /** Gate items, independent checks, and release runs that are preparing or running. */
  items: QueueItemView[];
  /** Buildsets the repository's scheduler is running. */
  runningBuildsets: number;
}

const inFlight = (view: QueueItemView) => view.item.state === "preparing" || view.item.state === "running";

/**
 * The work Quit would interrupt, by repository: the same rule the Tauri shell applies before it
 * asks for confirmation (`has_active_work`), so the confirmation lists what triggered it.
 */
export function activeWork(snapshot: AppSnapshot | undefined): RepositoryActiveWork[] {
  return (snapshot?.repositories ?? []).flatMap((repository) => {
    const items = [...repository.queue, ...repository.checks, ...(repository.release_runs ?? [])].filter(inFlight);
    const runningBuildsets = repository.resources.active_runs;
    if (items.length === 0 && runningBuildsets === 0) return [];
    return [{ repositoryId: repository.state.id, repositoryName: repository.state.name, items, runningBuildsets }];
  });
}
