//! `release` advance and push (staged-release-design.md sections 9 and 10) for repositories with
//! release-stage steps.
//!
//! A passing release run leaves a release certificate. An advance pass then moves `release` to
//! the newest certified `staging` commit and, with pushing enabled, publishes exactly that OID:
//!
//! 1. plan under the mutation lock: every R2 precondition, the newest certified target, and the
//!    on-disk configuration's release-stage digest;
//! 2. observe the remote outside the mutation lock (network unavailability only delays the
//!    advance);
//! 3. under the mutation lock again: re-plan, check the remote preflight, record one
//!    `release-advance` intent carrying the release intent and the frozen push, compare-and-swap
//!    `release` (verifying `staging`), and persist the advance with the intent `external-applied`
//!    (a push is owed) or `completed`;
//! 4. push outside the mutation lock with an exact lease, then settle the intent `completed` or
//!    `needs-attention` (`push-blocked`) under the mutation lock.
//!
//! Every pass holds the repository's `release_advance` lock, which `tg push`, `tg pull`, and
//! `tg reconcile` also take first, so none of them observes a push in flight. Each pass begins by
//! resuming an unfinished intent: a frozen push is always settled before any newer advance.

use super::*;
use tollgate_store::RELEASE_ADVANCE_INTENT_KIND;

/// Block code holding back release advances after a failed or divergent release push.
pub(crate) const PUSH_BLOCKED: &str = "push-blocked";
/// Block code holding back release advances while the remote is outside certified history.
pub(crate) const REMOTE_PREFLIGHT_MISMATCH: &str = "remote-preflight-mismatch";
/// Block code holding back release advances while the unreleased range has a commit Tollgate
/// neither promoted nor adopted.
pub(crate) const RELEASE_RANGE_UNATTRIBUTED: &str = "release-range-unattributed";
/// Pause code while `max_release_lag` holds promotion back.
pub(crate) const RELEASE_LAG: &str = "release-lag";

/// The durable boundaries of a release advance at which tests stop it as if the process crashed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ReleaseFault {
    /// After the release run passed and the advance was planned, before any durable write.
    BeforeIntent,
    /// After the release intent is recorded, before the local compare-and-swap.
    AfterIntent,
    /// After the local compare-and-swap, before the advance is persisted.
    AfterCas,
    /// After the advance is persisted, before the push.
    AfterLocalAdvance,
    /// After the push, before the intent completes.
    AfterPush,
}

/// Stops the advance at `point` when a test armed it there. Once fired, every later advance pass
/// of the runtime does nothing, as after a real crash, until the test restarts the service.
fn inject_release_fault(
    runtime: &RepositoryRuntime,
    point: ReleaseFault,
) -> Result<(), ServiceError> {
    #[cfg(test)]
    {
        let mut fault = runtime.release_fault.lock();
        if let Some((armed, fired)) = fault.as_mut()
            && *armed == point
            && !*fired
        {
            *fired = true;
            return Err(ServiceError::Invariant(format!(
                "injected release fault {point:?}"
            )));
        }
    }
    #[cfg(not(test))]
    let _ = (runtime, point);
    Ok(())
}

fn release_fault_fired(runtime: &RepositoryRuntime) -> bool {
    #[cfg(test)]
    {
        runtime.release_fault.lock().is_some_and(|(_, fired)| fired)
    }
    #[cfg(not(test))]
    {
        let _ = runtime;
        false
    }
}

/// One planned advance: every R2 precondition held under the mutation lock.
#[derive(Clone, Debug)]
pub(crate) struct ReleasePlan {
    certificate: PassCertificate,
    item_id: QueueItemId,
    expected_old: GitOid,
    staging: GitOid,
    target: GitOid,
    commits: usize,
    /// The configured remote and branch when pushing is enabled.
    push: Option<(String, String)>,
}

/// How a release push ended.
enum ReleasePushSettlement {
    Published,
    Blocked {
        observed_remote: Option<Option<GitOid>>,
        error: String,
    },
}

fn evidence_oid(evidence: &serde_json::Value, key: &str) -> Result<GitOid, ServiceError> {
    serde_json::from_value(
        evidence.get(key).cloned().ok_or_else(|| {
            ServiceError::Invariant(format!("release advance intent omitted {key}"))
        })?,
    )
    .map_err(|error| ServiceError::Invariant(format!("release advance intent {key}: {error}")))
}

fn evidence_str<'value>(
    evidence: &'value serde_json::Value,
    key: &str,
) -> Result<&'value str, ServiceError> {
    evidence
        .get(key)
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| ServiceError::Invariant(format!("release advance intent omitted {key}")))
}

/// Whether `state` holds the release block `code`.
pub(crate) fn release_held(state: &RepositoryState, code: &str) -> bool {
    state
        .release_block_reasons
        .iter()
        .any(|reason| reason.code == code)
}

impl TollgateService {
    /// Requests a release advance pass and spawns it, unless the repository is still activating
    /// (activation spawns one once the runtime is published) or the app is shutting down.
    /// Callers never hold the repository mutation lock.
    pub(crate) fn spawn_release_advance(
        self: &Arc<Self>,
        repository_id: RepositoryId,
        runtime: &Arc<RepositoryRuntime>,
    ) {
        runtime
            .release_advance_requested
            .store(true, Ordering::Release);
        if self.shutting_down.load(Ordering::Acquire)
            || ACTIVATING_REPOSITORY
                .try_with(|activating| *activating == repository_id)
                .unwrap_or(false)
        {
            return;
        }
        let service = Arc::clone(self);
        tokio::spawn(async move {
            if let Err(error) = service.advance_release(repository_id).await {
                eprintln!("Tollgate release advance for {repository_id} failed: {error}");
            }
        });
    }

    /// One release advance pass. Concurrent requests coalesce: a pass runs only when one was
    /// requested since the previous pass started. Afterwards promotion is re-evaluated, because a
    /// release advance or verdict can lift a `max_release_lag` pause.
    async fn advance_release(
        self: &Arc<Self>,
        repository_id: RepositoryId,
    ) -> Result<(), ServiceError> {
        let runtime = self.runtime(repository_id).await?;
        let advance = runtime.release_advance.lock().await;
        if !runtime
            .release_advance_requested
            .swap(false, Ordering::AcqRel)
            || release_fault_fired(&runtime)
        {
            return Ok(());
        }
        let result = self.advance_release_locked(&runtime).await;
        runtime
            .release_advance_passes
            .fetch_add(1, Ordering::AcqRel);
        drop(advance);
        result?;
        self.promote_ready(repository_id).await
    }

    async fn advance_release_locked(
        self: &Arc<Self>,
        runtime: &Arc<RepositoryRuntime>,
    ) -> Result<(), ServiceError> {
        if !self.resume_release_advance(runtime).await? {
            return Ok(());
        }
        let plan = {
            let _mutation = runtime.mutation.lock().await;
            self.plan_release_advance(runtime).await?
        };
        let Some(plan) = plan else {
            return Ok(());
        };
        let observed_remote = match &plan.push {
            Some((remote, branch)) => {
                let observed = match runtime.git.remote_url(remote, true).await {
                    Ok(push_url) => {
                        runtime
                            .git
                            .fetch_remote_ref(
                                &push_url,
                                branch,
                                &remote_observation_ref(remote, branch),
                            )
                            .await
                    }
                    Err(error) => Err(error),
                };
                match observed {
                    Ok(observed) => Some(observed),
                    Err(error) => {
                        eprintln!(
                            "Tollgate deferred the release advance to {} because the remote could not be observed: {error}",
                            plan.target.short()
                        );
                        return Ok(());
                    }
                }
            }
            None => None,
        };
        let owed_push = {
            let _mutation = runtime.mutation.lock().await;
            let Some(plan) = self.plan_release_advance(runtime).await? else {
                return Ok(());
            };
            self.advance_release_locally(runtime, plan, observed_remote)
                .await?
        };
        if let Some((command_id, evidence)) = owed_push {
            self.release_push_step(runtime, command_id, &evidence)
                .await?;
        }
        Ok(())
    }

    /// Settles an unfinished release advance before any newer one: a prepared intent recovers
    /// from the refs, and an owed or blocked push is observed and retried with its frozen lease.
    /// Returns whether no unfinished advance remains.
    async fn resume_release_advance(
        self: &Arc<Self>,
        runtime: &Arc<RepositoryRuntime>,
    ) -> Result<bool, ServiceError> {
        loop {
            let Some((command_id, _, evidence, intent_state)) = runtime
                .store
                .recoverable_operations(&[RELEASE_ADVANCE_INTENT_KIND])?
                .into_iter()
                .next()
            else {
                return Ok(true);
            };
            match intent_state {
                IntentState::Prepared => {
                    let _mutation = runtime.mutation.lock().await;
                    self.recover_prepared_release_advance(
                        runtime,
                        command_id,
                        &evidence,
                        Actor::App,
                    )
                    .await?;
                }
                IntentState::ExternalApplied | IntentState::NeedsAttention => {
                    self.release_push_step(runtime, command_id, &evidence)
                        .await?;
                    return Ok(runtime
                        .store
                        .recoverable_operations(&[RELEASE_ADVANCE_INTENT_KIND])?
                        .is_empty());
                }
                IntentState::Completed | IntentState::Canceled => return Ok(true),
            }
        }
    }

    /// Startup recovery of release advances (section 9), before the external-movement check:
    /// a prepared intent finalizes when `release` holds its new OID, cancels when `release`
    /// still holds the expected old OID, and otherwise cancels and leaves the moved `release` to
    /// external-movement reconciliation. An owed push resumes in the first advance pass.
    pub(crate) async fn reconcile_release_intents(
        self: &Arc<Self>,
        runtime: &Arc<RepositoryRuntime>,
    ) -> Result<(), ServiceError> {
        for (command_id, _, evidence, intent_state) in runtime
            .store
            .recoverable_operations(&[RELEASE_ADVANCE_INTENT_KIND])?
        {
            if intent_state == IntentState::Prepared {
                self.recover_prepared_release_advance(
                    runtime,
                    command_id,
                    &evidence,
                    Actor::Recovery,
                )
                .await?;
            }
        }
        Ok(())
    }

    /// Recovers a prepared release intent from the observed `release` (callers hold the
    /// mutation lock or run startup recovery).
    async fn recover_prepared_release_advance(
        self: &Arc<Self>,
        runtime: &Arc<RepositoryRuntime>,
        command_id: CommandId,
        evidence: &serde_json::Value,
        actor: Actor,
    ) -> Result<(), ServiceError> {
        let expected_old = evidence_oid(evidence, "expected_old")?;
        let new = evidence_oid(evidence, "new")?;
        let observed = runtime.git.release_oid().await?;
        match classify_release_intent(&expected_old, &new, &observed) {
            ReleaseIntentRecovery::Finalize => {
                self.finish_local_release_advance(runtime, command_id, evidence, actor)
                    .await
            }
            ReleaseIntentRecovery::NotApplied => {
                runtime.store.set_intent_state(
                    command_id,
                    IntentState::Canceled,
                    &serde_json::json!({"recovery": "release-cas-not-applied"}),
                )?;
                Ok(())
            }
            ReleaseIntentRecovery::Moved => {
                runtime.store.set_intent_state(
                    command_id,
                    IntentState::Canceled,
                    &serde_json::json!({
                        "recovery": "release-moved-externally",
                        "observed_release": observed,
                    }),
                )?;
                Ok(())
            }
        }
    }

    /// Plans an advance under the mutation lock (section 9, step 1). `None` means no advance is
    /// possible now: no release stage, a blocked or paused repository, a held release, an
    /// unfinished advance, moved refs, nothing unreleased, no certified target, an on-disk
    /// release stage that no longer matches the certificate, or unverifiable evidence.
    async fn plan_release_advance(
        self: &Arc<Self>,
        runtime: &Arc<RepositoryRuntime>,
    ) -> Result<Option<ReleasePlan>, ServiceError> {
        let (config, state) = {
            let data = runtime.data.lock();
            (data.config.clone(), data.state.clone())
        };
        let Some(release_digest) = config.release_step_graph_digest.clone() else {
            return Ok(None);
        };
        if state.execution_state != RepositoryExecutionState::Active
            || !state.release_block_reasons.is_empty()
            || !runtime
                .store
                .recoverable_operations(&[RELEASE_ADVANCE_INTENT_KIND])?
                .is_empty()
        {
            return Ok(None);
        }
        let observed = ObservedTollgateRefs::read(&runtime.git).await?;
        match self
            .verify_tollgate_refs(runtime, &observed, "before a release advance")
            .await
        {
            Ok(()) => {}
            Err(ServiceError::Invariant(message)) => {
                eprintln!("Tollgate held the release advance: {message}");
                return Ok(None);
            }
            Err(error) => return Err(error),
        }
        let state = runtime.data.lock().state.clone();
        if state.release_oid == state.staging_oid {
            return Ok(None);
        }
        let range = runtime
            .git
            .first_parent_commits_between(&state.release_oid, &state.staging_oid)
            .await?;
        let certified = {
            let data = runtime.data.lock();
            data.items
                .iter()
                .filter(|item| {
                    item.kind == QueueItemKind::Release && item.state == QueueItemState::CheckPassed
                })
                .filter_map(|item| {
                    let certificate = data
                        .certificates
                        .iter()
                        .find(|certificate| Some(certificate.id) == item.certificate_id)?;
                    (certificate.tested_oid == item.source_oid
                        && certificate.step_graph_digest == release_digest
                        && certificate.checkout_verified)
                        .then(|| {
                            (
                                certificate.tested_oid.clone(),
                                (certificate.clone(), item.id),
                            )
                        })
                })
                .collect::<HashMap<_, _>>()
        };
        let Some(index) = newest_release_target(&range, |oid| certified.contains_key(oid)) else {
            return Ok(None);
        };
        let target = range[index].clone();
        let (certificate, item_id) = certified[&target].clone();
        let disk_config = EffectiveConfig::parse(
            &tokio::fs::read_to_string(runtime.git.worktree_root.join(".tollgate/config.toml"))
                .await?,
        )?;
        if disk_config.release_step_graph_digest.as_deref()
            != Some(certificate.step_graph_digest.as_str())
        {
            eprintln!(
                "Tollgate held the release advance to {}: the on-disk release stage no longer matches its certificate",
                target.short()
            );
            return Ok(None);
        }
        if !self.release_range_attributed(runtime, &state.release_oid, &range[..=index])? {
            self.hold_release(
                runtime,
                RELEASE_RANGE_UNATTRIBUTED,
                format!(
                    "The unreleased range {}..{} has a commit Tollgate neither promoted nor adopted.",
                    state.release_oid.short(),
                    target.short()
                ),
                "Inspect staging's history, then run `tg reconcile` to adopt it explicitly.",
            )?;
            return Ok(None);
        }
        if let Err(error) = self
            .verify_release_certificate_logs(runtime, &certificate)
            .await
        {
            eprintln!(
                "Tollgate held the release advance to {}: {error}",
                target.short()
            );
            return Ok(None);
        }
        Ok(Some(ReleasePlan {
            certificate,
            item_id,
            expected_old: state.release_oid,
            staging: state.staging_oid,
            target,
            commits: index + 1,
            push: config
                .remote
                .enabled
                .then(|| (config.remote.name.clone(), config.remote.branch.clone())),
        }))
    }

    /// The local half of an advance under the mutation lock (section 9, steps 2–4): remote
    /// preflight, the release intent with its frozen push, the compare-and-swap, and the persisted
    /// advance. Returns the intent when a push is owed.
    async fn advance_release_locally(
        self: &Arc<Self>,
        runtime: &Arc<RepositoryRuntime>,
        plan: ReleasePlan,
        observed_remote: Option<Option<GitOid>>,
    ) -> Result<Option<(CommandId, serde_json::Value)>, ServiceError> {
        let repository_id = runtime.data.lock().state.id;
        let mut remote_already_published = false;
        let push = match &plan.push {
            None => serde_json::Value::Null,
            Some((remote, branch)) => {
                // The plan changed to a pushing configuration after the observation was skipped.
                let Some(observed) = observed_remote.clone() else {
                    return Ok(None);
                };
                let in_certified_history = match &observed {
                    Some(remote_oid) if remote_oid != &plan.target => {
                        runtime.git.is_ancestor(remote_oid, &plan.target).await?
                            && self.release_range_attributed(
                                runtime,
                                remote_oid,
                                &runtime
                                    .git
                                    .first_parent_commits_between(remote_oid, &plan.target)
                                    .await?,
                            )?
                    }
                    _ => false,
                };
                match classify_release_preflight(
                    observed.as_ref(),
                    &plan.target,
                    in_certified_history,
                ) {
                    ReleasePreflight::AlreadyPublished => {
                        remote_already_published = true;
                        serde_json::Value::Null
                    }
                    ReleasePreflight::Lease(expected_remote) => serde_json::json!({
                        "remote": remote,
                        "branch": branch,
                        "remote_fetch_url": runtime.git.remote_url(remote, false).await?,
                        "remote_push_url": runtime.git.remote_url(remote, true).await?,
                        "expected_remote": expected_remote,
                    }),
                    ReleasePreflight::Mismatch => {
                        self.hold_release(
                            runtime,
                            REMOTE_PREFLIGHT_MISMATCH,
                            format!(
                                "Remote {remote}/{branch} is at {}, which is not an ancestor of release target {} through Tollgate-certified history.",
                                observed
                                    .as_ref()
                                    .map(|oid| oid.short().to_string())
                                    .unwrap_or_else(|| "nothing".into()),
                                plan.target.short()
                            ),
                            "Run `tg pull` to adopt a remote fast-forward of staging, or `tg reconcile` after choosing the authoritative history.",
                        )?;
                        return Ok(None);
                    }
                }
            }
        };
        inject_release_fault(runtime, ReleaseFault::BeforeIntent)?;
        let command_id = CommandId::new();
        let evidence = serde_json::json!({
            "expected_old": plan.expected_old,
            "new": plan.target,
            "staging": plan.staging,
            "certificate_id": plan.certificate.id,
            "item_id": plan.item_id,
            "release_digest": plan.certificate.step_graph_digest,
            "commits": plan.commits,
            "push": push,
            "remote_already_published": remote_already_published,
        });
        runtime.store.prepare_operation(
            repository_id,
            RELEASE_ADVANCE_INTENT_KIND,
            command_id,
            &evidence,
        )?;
        if let Some((remote, branch)) = &plan.push
            && let Some(observed) = &observed_remote
        {
            runtime.store.record_remote_observation(
                repository_id,
                command_id,
                remote,
                &format!("refs/heads/{branch}"),
                observed.as_ref(),
                "release-preflight-fetch",
            )?;
        }
        let critical_bytes = runtime.data.lock().config.resources.volume_critical_bytes;
        if let Err(error) = self
            .reserve_runtime_volume(runtime, command_id, &runtime.git.common_dir, critical_bytes)
            .await
        {
            runtime.store.set_intent_state(
                command_id,
                IntentState::Canceled,
                &serde_json::json!({"stage": "release-volume-admission", "error": error.to_string()}),
            )?;
            eprintln!(
                "Tollgate deferred a release advance while storage admission is unavailable: {error}"
            );
            return Ok(None);
        }
        inject_release_fault(runtime, ReleaseFault::AfterIntent)?;
        if let Err(error) = runtime
            .git
            .compare_and_swap_release(&plan.expected_old, &plan.staging, &plan.target)
            .await
        {
            // The transaction moves nothing unless both refs held their expected OIDs.
            runtime.store.set_intent_state(
                command_id,
                IntentState::Canceled,
                &serde_json::json!({"stage": "release-cas", "error": error.to_string()}),
            )?;
            let observed = ObservedTollgateRefs::read(&runtime.git).await?;
            let _ = self
                .verify_tollgate_refs(runtime, &observed, "during a release advance")
                .await;
            return Err(error.into());
        }
        inject_release_fault(runtime, ReleaseFault::AfterCas)?;
        self.finish_local_release_advance(runtime, command_id, &evidence, Actor::App)
            .await?;
        inject_release_fault(runtime, ReleaseFault::AfterLocalAdvance)?;
        Ok((!evidence["push"].is_null()).then_some((command_id, evidence)))
    }

    /// Persists a `release` advance that the compare-and-swap applied: the new projection, the
    /// intent (`external-applied` while its push is owed, else `completed`), and
    /// `release.advanced`, in one transaction. User-owned `master` following `release` syncs.
    async fn finish_local_release_advance(
        self: &Arc<Self>,
        runtime: &Arc<RepositoryRuntime>,
        command_id: CommandId,
        evidence: &serde_json::Value,
        actor: Actor,
    ) -> Result<(), ServiceError> {
        let old = evidence_oid(evidence, "expected_old")?;
        let new = evidence_oid(evidence, "new")?;
        let push_owed = !evidence["push"].is_null();
        let mut state = runtime.data.lock().state.clone();
        state.release_oid = new.clone();
        self.refresh_release_projection(runtime, &mut state).await?;
        let push = if push_owed {
            "pending"
        } else if evidence["remote_already_published"].as_bool() == Some(true) {
            "already-published"
        } else {
            "disabled"
        };
        let event = runtime.store.record_release_advance(
            &state,
            command_id,
            if push_owed {
                IntentState::ExternalApplied
            } else {
                IntentState::Completed
            },
            &serde_json::json!({"release": new}),
            actor,
            "release.advanced",
            serde_json::json!({
                "from": old,
                "to": new,
                "certificate_id": evidence["certificate_id"],
                "item_id": evidence["item_id"],
                "commits": evidence["commits"],
                "release_state": state.release_state,
                "release_lag": state.release_lag,
                "push": push,
                "recovered": actor == Actor::Recovery,
            }),
        )?;
        {
            let mut data = runtime.data.lock();
            data.state.release_oid = state.release_oid;
            data.state.release_lag = state.release_lag;
            data.state.release_state = state.release_state;
            data.state.event_sequence = data.state.event_sequence.max(event.sequence);
        }
        let _ = runtime.events.send(event);
        if runtime.data.lock().config.sync_user_master == tollgate_config::SyncUserMaster::Release
            && let Err(error) = self
                .sync_user_master_after_promotion(runtime, None, &new, actor)
                .await
        {
            eprintln!(
                "Tollgate could not synchronize local master to release {}: {error}",
                new.short()
            );
        }
        Ok(())
    }

    /// Settles an owed or blocked release push (section 9, step 5) outside the mutation lock.
    /// It re-checks the frozen remote identity and observes the remote first, so a push that
    /// already landed completes and a diverged remote stays blocked; only a remote that still
    /// holds the lease is pushed, with that exact lease.
    async fn release_push_step(
        self: &Arc<Self>,
        runtime: &Arc<RepositoryRuntime>,
        command_id: CommandId,
        evidence: &serde_json::Value,
    ) -> Result<(), ServiceError> {
        let new = evidence_oid(evidence, "new")?;
        let push = evidence
            .get("push")
            .filter(|push| !push.is_null())
            .ok_or_else(|| ServiceError::Invariant("release advance owes no push".into()))?;
        let remote = evidence_str(push, "remote")?;
        let branch = evidence_str(push, "branch")?;
        let frozen_push_url = evidence_str(push, "remote_push_url")?;
        let expected_remote: Option<GitOid> =
            serde_json::from_value(push.get("expected_remote").cloned().unwrap_or_default())?;
        let settlement = if runtime.git.remote_url(remote, true).await.ok().as_deref()
            != Some(frozen_push_url)
        {
            ReleasePushSettlement::Blocked {
                observed_remote: None,
                error: format!(
                    "remote {remote} no longer resolves to the frozen push URL {frozen_push_url}"
                ),
            }
        } else {
            let observed = match runtime
                .git
                .observe_remote_ref(frozen_push_url, branch)
                .await
            {
                Ok(observed) => observed,
                Err(error) => {
                    eprintln!(
                        "Tollgate deferred the release push of {} because the remote could not be observed: {error}",
                        new.short()
                    );
                    return Ok(());
                }
            };
            match classify_release_push(expected_remote.as_ref(), &new, observed.as_ref()) {
                ReleasePushObservation::Published => ReleasePushSettlement::Published,
                ReleasePushObservation::Diverged => ReleasePushSettlement::Blocked {
                    error: format!(
                        "remote {remote}/{branch} moved to {} outside the frozen lease",
                        observed
                            .as_ref()
                            .map(|oid| oid.short().to_string())
                            .unwrap_or_else(|| "nothing".into())
                    ),
                    observed_remote: Some(observed),
                },
                ReleasePushObservation::Unchanged => match runtime
                    .git
                    .push_with_lease(frozen_push_url, branch, expected_remote.as_ref(), &new)
                    .await
                {
                    Ok(()) => {
                        inject_release_fault(runtime, ReleaseFault::AfterPush)?;
                        ReleasePushSettlement::Published
                    }
                    Err(push_error) => {
                        match runtime
                            .git
                            .observe_remote_ref(frozen_push_url, branch)
                            .await
                        {
                            Ok(observed)
                                if classify_release_push(
                                    expected_remote.as_ref(),
                                    &new,
                                    observed.as_ref(),
                                ) == ReleasePushObservation::Published =>
                            {
                                ReleasePushSettlement::Published
                            }
                            Ok(observed) => ReleasePushSettlement::Blocked {
                                observed_remote: Some(observed),
                                error: push_error.to_string(),
                            },
                            Err(observation_error) => ReleasePushSettlement::Blocked {
                                observed_remote: None,
                                error: format!(
                                    "{push_error}; the remote could not be re-observed: {observation_error}"
                                ),
                            },
                        }
                    }
                },
            }
        };
        self.settle_release_push(
            runtime,
            command_id,
            &new,
            expected_remote.as_ref(),
            settlement,
        )
        .await
    }

    async fn settle_release_push(
        self: &Arc<Self>,
        runtime: &Arc<RepositoryRuntime>,
        command_id: CommandId,
        new: &GitOid,
        expected_remote: Option<&GitOid>,
        settlement: ReleasePushSettlement,
    ) -> Result<(), ServiceError> {
        let _mutation = runtime.mutation.lock().await;
        let mut state = runtime.data.lock().state.clone();
        let was_blocked = release_held(&state, PUSH_BLOCKED);
        let event = match settlement {
            ReleasePushSettlement::Published => {
                state
                    .release_block_reasons
                    .retain(|reason| reason.code != PUSH_BLOCKED);
                runtime.store.record_release_advance(
                    &state,
                    command_id,
                    IntentState::Completed,
                    &serde_json::json!({"remote": new}),
                    Actor::App,
                    "release.pushed",
                    serde_json::json!({
                        "tested_oid": new,
                        "lease": expected_remote,
                        "recovered": was_blocked,
                        "notify": was_blocked,
                    }),
                )?
            }
            ReleasePushSettlement::Blocked {
                observed_remote,
                error,
            } => {
                if !was_blocked {
                    state.release_block_reasons.push(BlockReason {
                        code: PUSH_BLOCKED.into(),
                        message: format!("The release push of {} did not land: {error}", new.short()),
                        recovery_action: "Tollgate retries the exact leased push on the next release pass; run `tg push` to retry now, or `tg reconcile` to abandon the remote promise.".into(),
                    });
                }
                let mut observed = serde_json::json!({"error": error, "push": "blocked"});
                if let Some(remote) = &observed_remote {
                    observed["observed_remote_oid"] = serde_json::to_value(remote)?;
                }
                runtime.store.record_release_advance(
                    &state,
                    command_id,
                    IntentState::NeedsAttention,
                    &observed,
                    Actor::App,
                    "release.push-blocked",
                    serde_json::json!({
                        "tested_oid": new,
                        "lease": expected_remote,
                        "observed_remote": observed_remote,
                        "error": error,
                        "notify": !was_blocked,
                    }),
                )?
            }
        };
        {
            let mut data = runtime.data.lock();
            data.state.release_block_reasons = state.release_block_reasons;
            data.state.event_sequence = data.state.event_sequence.max(event.sequence);
        }
        let _ = runtime.events.send(event);
        Ok(())
    }

    /// Records a release block (callers hold the mutation lock). Release advances wait until
    /// `tg push`, `tg pull`, or `tg reconcile` clears it; `staging` promotions continue.
    pub(crate) fn hold_release(
        &self,
        runtime: &RepositoryRuntime,
        code: &str,
        message: String,
        recovery_action: &str,
    ) -> Result<(), ServiceError> {
        let mut state = runtime.data.lock().state.clone();
        if release_held(&state, code) {
            return Ok(());
        }
        state.release_block_reasons.push(BlockReason {
            code: code.into(),
            message: message.clone(),
            recovery_action: recovery_action.into(),
        });
        let event = runtime.store.record_state_event(
            &state,
            Actor::App,
            "release.held",
            serde_json::json!({"code": code, "message": message, "notify": true}),
        )?;
        {
            let mut data = runtime.data.lock();
            data.state.release_block_reasons = state.release_block_reasons;
            data.state.event_sequence = data.state.event_sequence.max(event.sequence);
        }
        let _ = runtime.events.send(event);
        Ok(())
    }

    /// R2's provenance rule over `range`, the first-parent commits after `base`: each was
    /// promoted to `staging` by Tollgate or adopted by an explicit reconciliation.
    pub(crate) fn release_range_attributed(
        &self,
        runtime: &RepositoryRuntime,
        base: &GitOid,
        range: &[GitOid],
    ) -> Result<bool, ServiceError> {
        if range.is_empty() {
            return Ok(true);
        }
        let promotions = runtime.store.promotion_edges()?;
        let adoptions = runtime
            .store
            .completed_operation_records("reconcile")?
            .into_iter()
            .filter_map(|(expected, _)| {
                let from: GitOid =
                    serde_json::from_value(expected.get("adoption_from")?.clone()).ok()?;
                let to: GitOid =
                    serde_json::from_value(expected.get("adoption_to")?.clone()).ok()?;
                Some((from.as_bytes().to_vec(), to.as_bytes().to_vec()))
            })
            .collect::<Vec<_>>();
        let range = range
            .iter()
            .map(|oid| oid.as_bytes().to_vec())
            .collect::<Vec<_>>();
        Ok(release_range_certified(
            &base.as_bytes().to_vec(),
            &range,
            &promotions,
            &adoptions,
        ))
    }

    /// Verifies the sealed logs of every voting result a release certificate names.
    async fn verify_release_certificate_logs(
        &self,
        runtime: &RepositoryRuntime,
        certificate: &PassCertificate,
    ) -> Result<(), ServiceError> {
        let generation = runtime
            .data
            .lock()
            .generations
            .iter()
            .find(|generation| generation.id == certificate.validation_generation_id)
            .cloned()
            .ok_or_else(|| {
                ServiceError::Invariant("release certificate generation is missing".into())
            })?;
        let config = self.configuration_for_generation(runtime, &generation)?;
        for result in &certificate.voting_results {
            let step = config
                .steps
                .iter()
                .find(|step| stable_step_id(certificate.buildset_id, &step.name) == result.step_id)
                .ok_or_else(|| {
                    ServiceError::Invariant(
                        "release certificate names an unknown voting step".into(),
                    )
                })?;
            let log_path = runtime
                .logs_root
                .join(certificate.buildset_id.to_string())
                .join(format!("{}.tlog", step.name));
            if !verify_durable_log(
                log_path,
                &result.log_hash,
                result.log_stdout_end,
                result.log_stderr_end,
            )
            .await?
            {
                return Err(ServiceError::Invariant(format!(
                    "sealed log evidence for release step {} failed integrity verification",
                    step.name
                )));
            }
        }
        Ok(())
    }

    /// Requires both Tollgate refs at their persisted OIDs (callers hold the mutation lock), with
    /// one exception for a repository with release-stage steps (section 10): an external
    /// fast-forward of `release` alone that stays a first-parent ancestor of `staging` is
    /// adopted as uncertified, and the next push still needs a passing release run. Anything
    /// else blocks the repository with its external-movement code and returns the error.
    pub(crate) async fn verify_tollgate_refs(
        &self,
        runtime: &RepositoryRuntime,
        observed: &ObservedTollgateRefs,
        when: &str,
    ) -> Result<(), ServiceError> {
        let (staged, persisted_staging, persisted_release) = {
            let data = runtime.data.lock();
            (
                data.config.has_release_stage(),
                data.state.staging_oid.clone(),
                data.state.release_oid.clone(),
            )
        };
        if staged
            && observed.staging == persisted_staging
            && observed.release != persisted_release
            && self
                .release_fast_forward_within_staging(runtime, &persisted_release, observed)
                .await?
        {
            return self
                .adopt_uncertified_release(runtime, &observed.release, when, Actor::App)
                .await;
        }
        block_externally_moved_refs(runtime, observed, when)
    }

    /// Whether observed `release` is a first-parent descendant of `persisted_release` and a
    /// first-parent ancestor of (or equal to) observed `staging`.
    pub(crate) async fn release_fast_forward_within_staging(
        &self,
        runtime: &RepositoryRuntime,
        persisted_release: &GitOid,
        observed: &ObservedTollgateRefs,
    ) -> Result<bool, ServiceError> {
        let within = runtime
            .git
            .first_parent_commits_between(persisted_release, &observed.staging)
            .await?
            .contains(&observed.release);
        Ok(
            classify_release_movement(persisted_release, &observed.release, within, within)
                == ReleaseMovement::AdoptUncertified,
        )
    }

    /// Adopts an external fast-forward of `release` as uncertified (section 10).
    pub(crate) async fn adopt_uncertified_release(
        &self,
        runtime: &RepositoryRuntime,
        observed_release: &GitOid,
        when: &str,
        actor: Actor,
    ) -> Result<(), ServiceError> {
        let mut state = runtime.data.lock().state.clone();
        let from = state.release_oid.clone();
        state.release_oid = observed_release.clone();
        self.refresh_release_projection(runtime, &mut state).await?;
        let event = runtime.store.record_state_event(
            &state,
            actor,
            "release.adopted-uncertified",
            serde_json::json!({
                "from": from,
                "to": observed_release,
                "when": when,
                "release_state": state.release_state,
            }),
        )?;
        {
            let mut data = runtime.data.lock();
            data.state.release_oid = state.release_oid;
            data.state.release_lag = state.release_lag;
            data.state.release_state = state.release_state;
            data.state.event_sequence = data.state.event_sequence.max(event.sequence);
        }
        let _ = runtime.events.send(event);
        Ok(())
    }

    /// Cancels every unfinished release advance, abandoning any frozen push. `tg reconcile` and a
    /// `tg pull` that adopts the remote use it (both hold the `release_advance` lock).
    pub(crate) fn abandon_release_advances(
        runtime: &RepositoryRuntime,
        evidence: &serde_json::Value,
    ) -> Result<(), ServiceError> {
        for (command_id, _, _, state) in runtime
            .store
            .recoverable_operations(&[RELEASE_ADVANCE_INTENT_KIND])?
        {
            if state == IntentState::NeedsAttention {
                runtime
                    .store
                    .cancel_attention_intent(command_id, evidence)?;
            } else {
                runtime
                    .store
                    .set_intent_state(command_id, IntentState::Canceled, evidence)?;
            }
        }
        Ok(())
    }

    /// Completes every unfinished release advance whose OID `published_release` contains, after
    /// `tg push` published it (callers hold both locks). Clears the push block.
    pub(crate) async fn complete_published_release_advances(
        &self,
        runtime: &RepositoryRuntime,
        published_release: &GitOid,
    ) -> Result<(), ServiceError> {
        for (command_id, _, evidence, _) in runtime
            .store
            .recoverable_operations(&[RELEASE_ADVANCE_INTENT_KIND])?
        {
            let new = evidence_oid(&evidence, "new")?;
            if !runtime.git.is_ancestor(&new, published_release).await? {
                continue;
            }
            let mut state = runtime.data.lock().state.clone();
            state
                .release_block_reasons
                .retain(|reason| reason.code != PUSH_BLOCKED);
            let event = runtime.store.record_release_advance(
                &state,
                command_id,
                IntentState::Completed,
                &serde_json::json!({"remote": published_release, "via": "tg push"}),
                Actor::Cli,
                "release.pushed",
                serde_json::json!({
                    "tested_oid": new,
                    "published": published_release,
                    "via": "tg push",
                    "recovered": true,
                    "notify": false,
                }),
            )?;
            let mut data = runtime.data.lock();
            data.state.release_block_reasons = state.release_block_reasons;
            data.state.event_sequence = data.state.event_sequence.max(event.sequence);
            drop(data);
            let _ = runtime.events.send(event);
        }
        Ok(())
    }

    /// Whether `oid` is the tested OID of a passed release run (any release-stage digest).
    pub(crate) fn release_certified(runtime: &RepositoryRuntime, oid: &GitOid) -> bool {
        let data = runtime.data.lock();
        data.certificates.iter().any(|certificate| {
            &certificate.tested_oid == oid
                && certificate.checkout_verified
                && data.items.iter().any(|item| {
                    item.id == certificate.queue_item_id
                        && item.kind == QueueItemKind::Release
                        && item.state == QueueItemState::CheckPassed
                })
        })
    }

    /// The `max_release_lag` pause (section 6), re-evaluated before each promotion attempt under
    /// the mutation lock. The queue head waits while the release stage is configured, the limit
    /// is positive, `release` trails `staging` by more than the limit, the latest release run
    /// failed, and the head was not approved with `--release-fix`. Changes persist with
    /// `promotion.paused` (which notifies) or `promotion.resumed`. Returns whether promotion
    /// waits.
    pub(crate) fn reconcile_promotion_pause(
        &self,
        runtime: &RepositoryRuntime,
    ) -> Result<bool, ServiceError> {
        let (decision, head, current) = {
            let data = runtime.data.lock();
            let head = data
                .items
                .iter()
                .filter(|item| item.kind == QueueItemKind::Gate && !item.state.is_terminal())
                .min_by_key(|item| item.enqueue_sequence)
                .cloned();
            let max = data.config.resources.max_release_lag;
            let lag = data.state.release_lag.commits;
            let decision = head
                .as_ref()
                .filter(|head| {
                    data.config.has_release_stage()
                        && head.promotion_authorized
                        && release_lag_pauses_promotion(
                            max,
                            lag,
                            latest_release_run_failed(&data),
                            head.release_fix,
                        )
                })
                .map(|_| (lag, max));
            (decision, head, data.state.promotion_pause.clone())
        };
        let (pause, kind, payload) = match (decision, current.is_some()) {
            (Some((lag, max)), false) => (
                Some(BlockReason {
                    code: RELEASE_LAG.into(),
                    message: format!(
                        "Promotion waits: release trails staging by {lag} commits, more than max_release_lag = {max}, and the latest release run failed."
                    ),
                    recovery_action: "Approve the fix with `tg approve --release-fix <candidate-id>`, or wait for a passing release run.".into(),
                }),
                "promotion.paused",
                serde_json::json!({
                    "reason": RELEASE_LAG,
                    "release_lag": lag,
                    "max_release_lag": max,
                    "item_id": head.as_ref().map(|head| head.id),
                    "notify": true,
                }),
            ),
            (None, true) => (
                None,
                "promotion.resumed",
                serde_json::json!({
                    "reason": RELEASE_LAG,
                    "item_id": head.as_ref().map(|head| head.id),
                    "release_fix": head.as_ref().is_some_and(|head| head.release_fix),
                }),
            ),
            (decision, _) => return Ok(decision.is_some()),
        };
        let paused = pause.is_some();
        let mut state = runtime.data.lock().state.clone();
        state.promotion_pause = pause;
        let event = runtime
            .store
            .record_state_event(&state, Actor::App, kind, payload)?;
        {
            let mut data = runtime.data.lock();
            data.state.promotion_pause = state.promotion_pause;
            data.state.event_sequence = data.state.event_sequence.max(event.sequence);
        }
        let _ = runtime.events.send(event);
        Ok(paused)
    }
}
