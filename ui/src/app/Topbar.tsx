import { motion } from "framer-motion";
import * as DropdownMenu from "@radix-ui/react-dropdown-menu";
import { ChevronDown, Command, FlaskConical, FolderGit2, GitCommitHorizontal, GitMerge, MoreHorizontal, Pause, Play, Plus, RefreshCw, UploadCloud } from "lucide-react";
import type { RepositorySnapshot } from "../lib/types";
import type { Route } from "./useAppState";
import { Badge } from "../components/ui/Badge";
import { Button } from "../components/ui/Button";
import { Tooltip } from "../components/ui/Tooltip";
import { repositoryStatus } from "../components/StatusGlyph";
import { shortId } from "../lib/utils";

export function Topbar({ repository, route, onCommand, onApprove, onCheck, onPull, onPush, onReconcile, onWorktrees, onPause, onRefresh, refreshing }: { repository?: RepositorySnapshot; route: Route; onCommand: () => void; onApprove: () => void; onCheck: () => void; onPull: () => void; onPush: () => void; onReconcile: () => void; onWorktrees: () => void; onPause: () => void; onRefresh: () => void; refreshing: boolean }) {
  const status = repository && repositoryStatus(repository.state.execution_state);
  return <header className="topbar" data-tauri-drag-region>
    <div className="topbar__title" data-tauri-drag-region>
      {repository ? <>
        <span>{repository.state.name}</span><span className="topbar__slash">/</span><strong>{route === "queue" ? "Gate" : route[0]?.toUpperCase() + route.slice(1)}</strong>
        <button className="topbar__branch" disabled title="Tollgate-owned local integration branch"><GitCommitHorizontal size={14} /><span>release</span><code>{shortId(repository.state.master_oid.bytes, 7)}</code><ChevronDown size={12} /></button>
        {status && <Badge tone={status.tone} dot>{status.label}</Badge>}
      </> : <strong>Tollgate</strong>}
    </div>
    <div className="topbar__actions">
      <button className="command-trigger" onClick={onCommand}><Command size={14} /><span>Command</span><kbd>⌘ K</kbd></button>
      {repository && <>
        <Tooltip label="Refresh service state"><Button size="icon" variant="ghost" onClick={onRefresh} aria-label="Refresh"><RefreshCw size={16} className={refreshing ? "spin" : ""} /></Button></Tooltip>
        <Tooltip label={repository.state.execution_state === "paused" ? "Resume gate" : repository.state.execution_state === "active" ? "Pause after active commands finish" : "Pause is unavailable in this repository state"}><Button size="icon" variant="ghost" onClick={onPause} disabled={!['active', 'paused'].includes(repository.state.execution_state)} aria-label={repository.state.execution_state === "paused" ? "Resume gate" : "Pause gate"}>{repository.state.execution_state === "paused" ? <Play size={16} /> : <Pause size={16} />}</Button></Tooltip>
        <Tooltip label="Validate a commit without changing the gate"><Button variant="ghost" size="sm" onClick={onCheck} disabled={repository.state.execution_state !== "active"}><FlaskConical size={15} />Check</Button></Tooltip>
        <Button variant="primary" size="sm" onClick={onApprove} disabled={repository.state.execution_state !== "active"}><Plus size={15} />Approve</Button>
        <DropdownMenu.Root><DropdownMenu.Trigger asChild><Button variant="ghost" size="icon" aria-label="More repository actions"><MoreHorizontal size={17} /></Button></DropdownMenu.Trigger><DropdownMenu.Portal><DropdownMenu.Content className="action-menu" align="end" sideOffset={6}><DropdownMenu.Label>Repository</DropdownMenu.Label><DropdownMenu.Item onSelect={onWorktrees}><FolderGit2 />Worktree operations<span>safe helpers</span></DropdownMenu.Item><DropdownMenu.Separator /><DropdownMenu.Label>Remote & recovery</DropdownMenu.Label><DropdownMenu.Item onSelect={onPull}><RefreshCw />Adopt remote into release<span>safe CAS</span></DropdownMenu.Item><DropdownMenu.Item onSelect={onPush}><UploadCloud />Push release to master<span>exact lease</span></DropdownMenu.Item><DropdownMenu.Separator /><DropdownMenu.Item onSelect={onReconcile}><GitMerge />Reconcile movement<span>preview</span></DropdownMenu.Item></DropdownMenu.Content></DropdownMenu.Portal></DropdownMenu.Root>
      </>}
    </div>
    <motion.div className="topbar__activity" initial={false} animate={{ scaleX: refreshing ? 1 : 0 }} />
  </header>;
}
