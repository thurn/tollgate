import { Command as CommandPrimitive } from "cmdk";
import * as Dialog from "@radix-ui/react-dialog";
import { AnimatePresence, motion } from "framer-motion";
import { Archive, CircleGauge, Clock3, FileCode2, FlaskConical, FolderGit2, GitMerge, HeartPulse, Pause, Play, Plus, RefreshCw, Search, UploadCloud } from "lucide-react";
import type { Route } from "./useAppState";

type Props = {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  onRoute: (route: Route) => void;
  onApprove?: () => void;
  onCheck?: () => void;
  onPull?: () => void;
  onPush?: () => void;
  onReconcile?: () => void;
  onWorktrees?: () => void;
  onPause: () => void;
  paused: boolean;
  canPause: boolean;
  onReload: () => void;
};

export function CommandPalette({ open, onOpenChange, onRoute, onApprove, onCheck, onPull, onPush, onReconcile, onWorktrees, onPause, paused, canPause, onReload }: Props) {
  const run = (action: () => void) => { action(); onOpenChange(false); };
  return <Dialog.Root open={open} onOpenChange={onOpenChange}><AnimatePresence>{open && <Dialog.Portal forceMount>
    <Dialog.Overlay asChild><motion.div className="command-overlay" initial={{ opacity: 0 }} animate={{ opacity: 1 }} exit={{ opacity: 0 }} /></Dialog.Overlay>
    <Dialog.Content asChild aria-describedby={undefined}><motion.div className="command-dialog" initial={{ opacity: 0, y: -10, x: "-50%", scale: .98 }} animate={{ opacity: 1, y: 0, x: "-50%", scale: 1 }} exit={{ opacity: 0, y: -6, x: "-50%", scale: .99 }} transition={{ type: "spring", stiffness: 440, damping: 36 }}>
      <Dialog.Title className="sr-only">Tollgate commands</Dialog.Title>
      <CommandPrimitive label="Command menu">
        <div className="command-input"><Search size={17} /><CommandPrimitive.Input autoFocus placeholder="Type a command or search…" /><kbd>esc</kbd></div>
        <CommandPrimitive.List>
          <CommandPrimitive.Empty>No matching command.</CommandPrimitive.Empty>
          <CommandPrimitive.Group heading="Actions">
            {onApprove && <CommandPrimitive.Item onSelect={() => run(onApprove)}><Plus />Approve a change<span>⌘ ↵</span></CommandPrimitive.Item>}
            {onCheck && <CommandPrimitive.Item onSelect={() => run(onCheck)}><FlaskConical />Run independent check</CommandPrimitive.Item>}
            {onPull && <CommandPrimitive.Item onSelect={() => run(onPull)}><RefreshCw />Pull remote fast-forward</CommandPrimitive.Item>}
            {onPush && <CommandPrimitive.Item onSelect={() => run(onPush)}><UploadCloud />Push certified master</CommandPrimitive.Item>}
            {onReconcile && <CommandPrimitive.Item onSelect={() => run(onReconcile)}><GitMerge />Reconcile external movement</CommandPrimitive.Item>}
            {onWorktrees && <CommandPrimitive.Item onSelect={() => run(onWorktrees)}><FolderGit2 />Create, update, or remove worktree</CommandPrimitive.Item>}
            {canPause && <CommandPrimitive.Item onSelect={() => run(onPause)}>{paused ? <Play /> : <Pause />}{paused ? "Resume gate" : "Pause gate"}</CommandPrimitive.Item>}
            <CommandPrimitive.Item onSelect={() => run(onReload)}><RefreshCw />Reload shell environment</CommandPrimitive.Item>
          </CommandPrimitive.Group>
          <CommandPrimitive.Group heading="Navigate">
            <CommandPrimitive.Item onSelect={() => run(() => onRoute("queue"))}><GitMerge />Gate</CommandPrimitive.Item>
            <CommandPrimitive.Item onSelect={() => run(() => onRoute("history"))}><Clock3 />History</CommandPrimitive.Item>
            <CommandPrimitive.Item onSelect={() => run(() => onRoute("resources"))}><CircleGauge />Resources</CommandPrimitive.Item>
            <CommandPrimitive.Item onSelect={() => run(() => onRoute("storage"))}><Archive />Storage</CommandPrimitive.Item>
            <CommandPrimitive.Item onSelect={() => run(() => onRoute("configuration"))}><FileCode2 />Configuration</CommandPrimitive.Item>
            <CommandPrimitive.Item onSelect={() => run(() => onRoute("doctor"))}><HeartPulse />Doctor</CommandPrimitive.Item>
          </CommandPrimitive.Group>
        </CommandPrimitive.List>
      </CommandPrimitive>
    </motion.div></Dialog.Content>
  </Dialog.Portal>}</AnimatePresence></Dialog.Root>;
}
