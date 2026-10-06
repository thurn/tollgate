import { useEffect, useRef, useState, type KeyboardEvent } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { listen } from "@tauri-apps/api/event";
import { Power } from "lucide-react";
import { confirmQuit, getSnapshot, QUIT_CONFIRMATION_EVENT } from "../lib/api";
import type { QueueItemKind } from "../lib/types";
import { isTauri } from "../lib/utils";
import { Button } from "../components/ui/Button";
import { activeWork } from "./activeWork";

const kindLabel: Record<QueueItemKind, string> = { gate: "Gate", "independent-check": "Check", release: "Release run" };

/**
 * The confirmation Quit asks for while work is active. The Tauri shell keeps running and emits
 * {@link QUIT_CONFIRMATION_EVENT}; this lists the live snapshot's active work and either calls
 * `confirm_quit`, which shuts down and exits, or dismisses the request so the app keeps running.
 */
export function QuitConfirmation() {
  const queryClient = useQueryClient();
  const [open, setOpen] = useState(false);
  const snapshot = useQuery({ queryKey: ["snapshot"], queryFn: getSnapshot, enabled: open });
  const quit = useMutation({ mutationFn: confirmQuit });
  const cancelButton = useRef<HTMLButtonElement>(null);
  const dialog = useRef<HTMLDivElement>(null);
  const quitting = quit.isPending || quit.isSuccess;

  useEffect(() => {
    if (!isTauri()) return;
    let active = true;
    let stop: undefined | (() => void);
    void listen(QUIT_CONFIRMATION_EVENT, () => {
      setOpen(true);
      void queryClient.invalidateQueries({ queryKey: ["snapshot"] });
    }).then((unlisten) => {
      if (active) stop = unlisten;
      else unlisten();
    });
    return () => { active = false; stop?.(); };
  }, [queryClient]);

  useEffect(() => {
    if (!open) return;
    const returnFocus = document.activeElement instanceof HTMLElement ? document.activeElement : null;
    cancelButton.current?.focus();
    return () => returnFocus?.focus();
  }, [open]);

  if (!open) return null;

  function cancel() {
    if (quitting) return;
    quit.reset();
    setOpen(false);
  }

  function onKeyDown(event: KeyboardEvent<HTMLDivElement>) {
    if (event.key === "Escape") { event.preventDefault(); cancel(); return; }
    if (event.key !== "Tab") return;
    const focusable = [...(dialog.current?.querySelectorAll<HTMLElement>("button:not(:disabled)") ?? [])];
    const first = focusable[0];
    const last = focusable[focusable.length - 1];
    if (!first || !last) { event.preventDefault(); return; }
    if (event.shiftKey && document.activeElement === first) { event.preventDefault(); last.focus(); }
    else if (!event.shiftKey && document.activeElement === last) { event.preventDefault(); first.focus(); }
  }

  const work = activeWork(snapshot.data);
  return <div className="quit-confirm__backdrop">
    <div ref={dialog} className="quit-confirm" role="alertdialog" aria-modal="true" aria-labelledby="quit-confirm-title" aria-describedby="quit-confirm-impact" onKeyDown={onKeyDown}>
      <h2 id="quit-confirm-title">Quit Tollgate and interrupt active work?</h2>
      <p id="quit-confirm-impact">Quitting stops every running buildset. Interrupted work reruns from the beginning when Tollgate next opens.</p>
      {!snapshot.data
        ? <p className="quit-confirm__note" role="status">{snapshot.isError ? "Tollgate could not list the active work." : "Checking active work…"}</p>
        : work.length === 0
          ? <p className="quit-confirm__note" role="status">The work has finished; nothing is active now.</p>
          : <ul className="quit-confirm__work" aria-label="Active work">
            {work.map((repository) => <li key={repository.repositoryId}>
              <strong>{repository.repositoryName}</strong>
              {repository.runningBuildsets > 0 && <span className="quit-confirm__count">{repository.runningBuildsets} {repository.runningBuildsets === 1 ? "buildset" : "buildsets"} running</span>}
              {repository.items.length > 0 && <ul aria-label={`${repository.repositoryName} active items`}>
                {repository.items.map((view) => <li key={view.item.id}>
                  <span className="quit-confirm__kind">{kindLabel[view.item.kind]}</span>
                  <span className="quit-confirm__subject">{view.item.metadata.subject}</span>
                  <span className="quit-confirm__state">{view.item.state === "preparing" ? "Preparing" : "Running"}</span>
                </li>)}
              </ul>}
            </li>)}
          </ul>}
      {quit.isError && <p className="release-feedback release-feedback--error" role="alert">{quit.error instanceof Error ? quit.error.message : String(quit.error)}</p>}
      {quit.isSuccess && <p className="quit-confirm__note" role="status">Stopping active work and quitting…</p>}
      <div className="quit-confirm__actions">
        <Button ref={cancelButton} size="sm" variant="ghost" onClick={cancel} disabled={quitting}>Keep running</Button>
        <Button size="sm" variant="danger" onClick={() => quit.mutate()} loading={quitting}><Power aria-hidden />Quit</Button>
      </div>
    </div>
  </div>;
}
