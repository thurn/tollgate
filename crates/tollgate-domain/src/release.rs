//! Staged-release decisions (technical-design.md R1–R5 and sections 10.5 and 10.9): which certified
//! `staging` commit `release` may advance to, how an interrupted advance recovers, how external
//! `release` movement is classified, and when `max_release_lag` pauses promotion.
//!
//! Every function is pure and generic over the commit identity, so the service applies them to
//! Git object IDs and the property tests below drive them through random interleavings of
//! promotions, release runs, crashes, pushes, and pulls.

/// R2's provenance rule: every commit in `range` (the first-parent commits after `base`, oldest
/// first) was promoted to `staging` by Tollgate or adopted by an explicit reconciliation.
///
/// A promotion edge `(old, new)` covers `new` when `old` is its parent in the chain. An adoption
/// edge `(from, to)` covers every commit after `from` up to and including `to`.
pub fn release_range_certified<T: PartialEq>(
    base: &T,
    range: &[T],
    promotions: &[(T, T)],
    adoptions: &[(T, T)],
) -> bool {
    let mut parent = base;
    let mut index = 0;
    while index < range.len() {
        let commit = &range[index];
        if promotions
            .iter()
            .any(|(old, new)| old == parent && new == commit)
        {
            parent = commit;
            index += 1;
            continue;
        }
        let adopted_through = adoptions
            .iter()
            .filter(|(from, _)| from == parent)
            .filter_map(|(_, to)| range[index..].iter().position(|candidate| candidate == to))
            .max();
        match adopted_through {
            Some(offset) => {
                parent = &range[index + offset];
                index += offset + 1;
            }
            None => return false,
        }
    }
    true
}

/// The newest release target: the index of the last commit in `range` (the first-parent commits
/// of `release..staging`, oldest first) that `certified` accepts. Release only ever advances to an
/// exact certified OID, never past it.
pub fn newest_release_target<T>(range: &[T], certified: impl Fn(&T) -> bool) -> Option<usize> {
    range.iter().rposition(certified)
}

/// How an unfinished release intent recovers from the observed `release` OID (technical-design.md
/// 10.9).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReleaseIntentRecovery {
    /// `release` holds the intent's new OID: the compare-and-swap applied, so finalize.
    Finalize,
    /// `release` still holds the expected old OID: nothing moved, so cancel; the next advance
    /// pass re-derives the target from the preconditions that hold then.
    NotApplied,
    /// `release` holds neither: external movement, which reconciliation classifies.
    Moved,
}

pub fn classify_release_intent<T: PartialEq>(
    expected_old: &T,
    new: &T,
    observed_release: &T,
) -> ReleaseIntentRecovery {
    if observed_release == new {
        ReleaseIntentRecovery::Finalize
    } else if observed_release == expected_old {
        ReleaseIntentRecovery::NotApplied
    } else {
        ReleaseIntentRecovery::Moved
    }
}

/// The remote branch observed after (or instead of) a leased release push.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReleasePushObservation {
    /// The remote holds the pushed OID.
    Published,
    /// The remote still holds the lease's expected value: the push had no effect and may be
    /// retried with the same lease.
    Unchanged,
    /// The remote holds something else: push-blocked until `tg push` or `tg reconcile`.
    Diverged,
}

pub fn classify_release_push<T: PartialEq>(
    expected_remote: Option<&T>,
    new: &T,
    observed_remote: Option<&T>,
) -> ReleasePushObservation {
    if observed_remote == Some(new) {
        ReleasePushObservation::Published
    } else if observed_remote == expected_remote {
        ReleasePushObservation::Unchanged
    } else {
        ReleasePushObservation::Diverged
    }
}

/// The remote preflight of a release advance (technical-design.md 10.9, step 4).
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReleasePreflight<T> {
    /// The remote already holds the target; the advance needs no push.
    AlreadyPublished,
    /// Push the target with an exact lease on this observed remote OID.
    Lease(T),
    /// The remote is missing, or is not an ancestor of the target through Tollgate-certified
    /// history: the advance is held back (`remote-preflight-mismatch`).
    Mismatch,
}

/// `remote_in_certified_history` is whether the observed remote OID is an ancestor of `new` and
/// every first-parent commit after it up to `new` is covered by [`release_range_certified`].
pub fn classify_release_preflight<T: PartialEq + Clone>(
    observed_remote: Option<&T>,
    new: &T,
    remote_in_certified_history: bool,
) -> ReleasePreflight<T> {
    match observed_remote {
        Some(remote) if remote == new => ReleasePreflight::AlreadyPublished,
        Some(remote) if remote_in_certified_history => ReleasePreflight::Lease(remote.clone()),
        _ => ReleasePreflight::Mismatch,
    }
}

/// External movement of `release` in a repository with release-stage steps (technical-design.md
/// 10.5).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReleaseMovement {
    Unchanged,
    /// A fast-forward that stays an ancestor of (or equal to) `staging`: accepted as an
    /// uncertified adoption. The next push still needs a passing release run.
    AdoptUncertified,
    /// Anything else blocks the repository for `tg reconcile`.
    Block,
}

pub fn classify_release_movement<T: PartialEq>(
    persisted: &T,
    observed: &T,
    fast_forward: bool,
    within_staging: bool,
) -> ReleaseMovement {
    if observed == persisted {
        ReleaseMovement::Unchanged
    } else if fast_forward && within_staging {
        ReleaseMovement::AdoptUncertified
    } else {
        ReleaseMovement::Block
    }
}

/// The `max_release_lag` safety valve (technical-design.md 10.9): with a positive limit, promotion
/// to `staging` waits while `release` trails it by more than the limit and the latest release run
/// failed. A candidate approved with `--release-fix` is never held back, so the fix for a red
/// release can always land.
pub fn release_lag_pauses_promotion(
    max_release_lag: u32,
    lag_commits: u64,
    latest_release_run_failed: bool,
    release_fix: bool,
) -> bool {
    max_release_lag > 0
        && lag_commits > u64::from(max_release_lag)
        && latest_release_run_failed
        && !release_fix
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn promotion_and_adoption_edges_certify_a_range() {
        let promotions = [(0, 1), (1, 2), (4, 5)];
        let adoptions = [(2, 4)];
        assert!(release_range_certified(
            &0,
            &[1, 2],
            &promotions,
            &adoptions
        ));
        assert!(release_range_certified(
            &0,
            &[1, 2, 3, 4, 5],
            &promotions,
            &adoptions
        ));
        assert!(release_range_certified(&0, &[], &promotions, &adoptions));
        // Commit 3 alone is neither promoted nor the end of an adoption from its parent.
        assert!(!release_range_certified(&2, &[3], &[], &[]));
        assert!(!release_range_certified(&1, &[2, 3], &promotions, &[]));
        // An edge from another parent never covers a commit.
        assert!(!release_range_certified(&7, &[2], &promotions, &adoptions));
    }

    #[test]
    fn recovery_and_movement_classification() {
        assert_eq!(
            classify_release_intent(&1, &2, &2),
            ReleaseIntentRecovery::Finalize
        );
        assert_eq!(
            classify_release_intent(&1, &2, &1),
            ReleaseIntentRecovery::NotApplied
        );
        assert_eq!(
            classify_release_intent(&1, &2, &3),
            ReleaseIntentRecovery::Moved
        );
        assert_eq!(
            classify_release_push(Some(&1), &2, Some(&2)),
            ReleasePushObservation::Published
        );
        assert_eq!(
            classify_release_push(Some(&1), &2, Some(&1)),
            ReleasePushObservation::Unchanged
        );
        assert_eq!(
            classify_release_push(Some(&1), &2, None),
            ReleasePushObservation::Diverged
        );
        assert_eq!(
            classify_release_preflight(Some(&2), &2, false),
            ReleasePreflight::AlreadyPublished
        );
        assert_eq!(
            classify_release_preflight(Some(&1), &2, true),
            ReleasePreflight::Lease(1)
        );
        assert_eq!(
            classify_release_preflight(Some(&1), &2, false),
            ReleasePreflight::Mismatch
        );
        assert_eq!(
            classify_release_preflight(None, &2, true),
            ReleasePreflight::Mismatch
        );
        assert_eq!(
            classify_release_movement(&1, &1, false, false),
            ReleaseMovement::Unchanged
        );
        assert_eq!(
            classify_release_movement(&1, &2, true, true),
            ReleaseMovement::AdoptUncertified
        );
        assert_eq!(
            classify_release_movement(&1, &2, true, false),
            ReleaseMovement::Block
        );
        assert_eq!(
            classify_release_movement(&1, &2, false, true),
            ReleaseMovement::Block
        );
    }

    #[test]
    fn release_lag_pauses_only_past_the_limit_after_a_failure() {
        assert!(!release_lag_pauses_promotion(0, 100, true, false));
        assert!(!release_lag_pauses_promotion(2, 2, true, false));
        assert!(release_lag_pauses_promotion(2, 3, true, false));
        assert!(!release_lag_pauses_promotion(2, 3, false, false));
        assert!(!release_lag_pauses_promotion(2, 3, true, true));
    }

    /// Where an advance stops when the process "crashes" (technical-design.md 21.3).
    #[derive(Clone, Copy, Debug)]
    enum Crash {
        None,
        AfterIntent,
        AfterCas,
        AfterPersist,
        AfterPush,
    }

    #[derive(Clone, Debug)]
    enum Op {
        Promote { release_fix: bool },
        AdoptExternalStaging,
        ReleaseRun { pass: bool },
        Advance { crash: Crash, push_fails: bool },
        Restart,
        Pull,
        TgPush,
        ExternalReleaseFastForward,
        ToggleStage,
    }

    fn op() -> impl Strategy<Value = Op> {
        let crash = prop_oneof![
            Just(Crash::None),
            Just(Crash::AfterIntent),
            Just(Crash::AfterCas),
            Just(Crash::AfterPersist),
            Just(Crash::AfterPush),
        ];
        prop_oneof![
            4 => any::<bool>().prop_map(|release_fix| Op::Promote { release_fix }),
            1 => Just(Op::AdoptExternalStaging),
            3 => any::<bool>().prop_map(|pass| Op::ReleaseRun { pass }),
            4 => (crash, any::<bool>())
                .prop_map(|(crash, push_fails)| Op::Advance { crash, push_fails }),
            2 => Just(Op::Restart),
            1 => Just(Op::Pull),
            1 => Just(Op::TgPush),
            1 => Just(Op::ExternalReleaseFastForward),
            1 => Just(Op::ToggleStage),
        ]
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    enum IntentState {
        Prepared,
        ExternalApplied,
        NeedsAttention { retryable: bool },
    }

    #[derive(Clone, Debug)]
    struct Intent {
        expected_old: u32,
        new: u32,
        expected_remote: Option<u32>,
        state: IntentState,
    }

    /// A model repository: commits are integers on one linear `staging` chain. Refs, the remote,
    /// and the durable store change only through the same decisions the service makes.
    #[derive(Debug)]
    struct World {
        chain: Vec<u32>,
        next: u32,
        staging: u32,
        release: u32,
        remote: Option<u32>,
        persisted_release: u32,
        promotions: Vec<(u32, u32)>,
        adoptions: Vec<(u32, u32)>,
        certified: Vec<u32>,
        latest_failed: bool,
        intent: Option<Intent>,
        release_stage: bool,
        max_release_lag: u32,
        blocked: bool,
        push_blocked: bool,
        /// Every OID `release` legitimately held: certified advances, the opt-out transaction,
        /// remote pulls, and uncertified adoptions.
        release_history: Vec<u32>,
        /// Every OID Tollgate pushed, with the lease it used.
        pushes: Vec<(Option<u32>, u32)>,
        remote_history: Vec<u32>,
    }

    impl World {
        fn new(max_release_lag: u32) -> Self {
            Self {
                chain: vec![0],
                next: 1,
                staging: 0,
                release: 0,
                remote: Some(0),
                persisted_release: 0,
                promotions: Vec::new(),
                adoptions: Vec::new(),
                certified: Vec::new(),
                latest_failed: false,
                intent: None,
                release_stage: true,
                max_release_lag,
                blocked: false,
                push_blocked: false,
                release_history: vec![0],
                pushes: Vec::new(),
                remote_history: vec![0],
            }
        }

        fn position(&self, commit: u32) -> usize {
            self.chain
                .iter()
                .position(|candidate| *candidate == commit)
                .unwrap()
        }

        fn range(&self, from: u32, to: u32) -> Vec<u32> {
            self.chain[self.position(from) + 1..=self.position(to)].to_vec()
        }

        fn lag(&self) -> u64 {
            (self.position(self.staging) - self.position(self.release)) as u64
        }

        fn append(&mut self) -> u32 {
            let commit = self.next;
            self.next += 1;
            self.chain.push(commit);
            commit
        }

        fn ancestor(&self, ancestor: u32, descendant: u32) -> bool {
            self.chain.contains(&ancestor)
                && self.chain.contains(&descendant)
                && self.position(ancestor) <= self.position(descendant)
        }

        fn promote(&mut self, release_fix: bool) {
            if self.blocked || self.intent.as_ref().is_some_and(|_| !self.release_stage) {
                return;
            }
            if self.release_stage
                && release_lag_pauses_promotion(
                    self.max_release_lag,
                    self.lag(),
                    self.latest_failed,
                    release_fix,
                )
            {
                return;
            }
            let old = self.staging;
            let commit = self.append();
            self.promotions.push((old, commit));
            self.staging = commit;
            if !self.release_stage {
                // R5: the opt-out two-ref transaction moves both refs together; the push
                // follows the promotion as before.
                self.release = commit;
                self.persisted_release = commit;
                self.release_history.push(commit);
                // R5: without a release stage, `release` equals `staging` after every promotion.
                assert_eq!(self.release, self.staging);
                if !self.push_blocked && self.remote == Some(old) {
                    self.pushes.push((Some(old), commit));
                    self.remote = Some(commit);
                    self.remote_history.push(commit);
                }
            }
        }

        fn release_run(&mut self, pass: bool) {
            let before = (self.staging, self.release, self.remote);
            if pass {
                self.certified.push(self.staging);
            }
            self.latest_failed = !pass;
            // R3: a release outcome never moves a ref.
            assert_eq!(before, (self.staging, self.release, self.remote));
        }

        fn eligible_target(&self) -> Option<u32> {
            let range = self.range(self.release, self.staging);
            let index = newest_release_target(&range, |commit| self.certified.contains(commit))?;
            let target = range[index];
            release_range_certified(
                &self.release,
                &range[..=index],
                &self.promotions,
                &self.adoptions,
            )
            .then_some(target)
        }

        /// The service's advance pass: resume an unfinished intent, then start a new one.
        fn advance(&mut self, crash: Crash, push_fails: bool) {
            if !self.release_stage || self.blocked {
                return;
            }
            if let Some(intent) = self.intent.clone() {
                match intent.state {
                    IntentState::ExternalApplied
                    | IntentState::NeedsAttention { retryable: true } => {
                        self.push_step(intent, crash, push_fails);
                    }
                    IntentState::NeedsAttention { retryable: false } => {}
                    IntentState::Prepared => unreachable!("recovery settles prepared intents"),
                }
                return;
            }
            if self.push_blocked {
                return;
            }
            let Some(new) = self.eligible_target() else {
                return;
            };
            let preflight = classify_release_preflight(
                self.remote.as_ref(),
                &new,
                self.remote.is_some_and(|remote| {
                    self.ancestor(remote, new)
                        && release_range_certified(
                            &remote,
                            &self.range(remote, new),
                            &self.promotions,
                            &self.adoptions,
                        )
                }),
            );
            let expected_remote = match preflight {
                ReleasePreflight::AlreadyPublished => None,
                ReleasePreflight::Lease(remote) => Some(remote),
                ReleasePreflight::Mismatch => {
                    self.push_blocked = true;
                    return;
                }
            };
            let intent = Intent {
                expected_old: self.release,
                new,
                expected_remote,
                state: IntentState::Prepared,
            };
            self.intent = Some(intent.clone());
            if matches!(crash, Crash::AfterIntent) {
                return;
            }
            self.release = new;
            if matches!(crash, Crash::AfterCas) {
                return;
            }
            self.finalize_local();
            if matches!(crash, Crash::AfterPersist) {
                return;
            }
            let intent = self.intent.clone();
            if let Some(intent) = intent {
                self.push_step(intent, crash, push_fails);
            }
        }

        /// The durable local half: persist `release` and complete the intent, or leave it
        /// external-applied while the frozen push is owed.
        fn finalize_local(&mut self) {
            let intent = self.intent.clone().unwrap();
            // R2: the new OID is a certified tested OID, a descendant of the old `release`
            // inside `staging`, and every commit up to it was promoted or adopted.
            assert!(self.certified.contains(&intent.new), "{self:?}");
            assert!(self.ancestor(intent.expected_old, intent.new), "{self:?}");
            assert!(self.ancestor(intent.new, self.staging), "{self:?}");
            assert!(
                release_range_certified(
                    &intent.expected_old,
                    &self.range(intent.expected_old, intent.new),
                    &self.promotions,
                    &self.adoptions,
                ),
                "{self:?}"
            );
            let intent = self.intent.as_mut().unwrap();
            self.persisted_release = intent.new;
            self.release_history.push(intent.new);
            if intent.expected_remote.is_none() {
                self.intent = None;
            } else {
                intent.state = IntentState::ExternalApplied;
            }
        }

        fn push_step(&mut self, intent: Intent, crash: Crash, push_fails: bool) {
            match classify_release_push(
                intent.expected_remote.as_ref(),
                &intent.new,
                self.remote.as_ref(),
            ) {
                ReleasePushObservation::Published => {
                    self.intent = None;
                    self.push_blocked = false;
                    return;
                }
                ReleasePushObservation::Diverged => {
                    self.intent.as_mut().unwrap().state =
                        IntentState::NeedsAttention { retryable: false };
                    self.push_blocked = true;
                    return;
                }
                ReleasePushObservation::Unchanged => {}
            }
            if push_fails {
                self.intent.as_mut().unwrap().state =
                    IntentState::NeedsAttention { retryable: true };
                self.push_blocked = true;
                return;
            }
            // R4: the leased push publishes a certified OID and applies only while the remote
            // holds the lease.
            assert!(self.certified.contains(&intent.new), "{self:?}");
            assert_eq!(
                self.remote, intent.expected_remote,
                "push without an exact lease"
            );
            self.pushes.push((intent.expected_remote, intent.new));
            self.remote = Some(intent.new);
            self.remote_history.push(intent.new);
            if matches!(crash, Crash::AfterPush) {
                return;
            }
            self.intent = None;
            self.push_blocked = false;
        }

        /// Startup recovery of a prepared intent, then the external-movement check.
        fn restart(&mut self) {
            if let Some(intent) = self.intent.clone()
                && intent.state == IntentState::Prepared
            {
                match classify_release_intent(&intent.expected_old, &intent.new, &self.release) {
                    ReleaseIntentRecovery::Finalize => self.finalize_local(),
                    ReleaseIntentRecovery::NotApplied | ReleaseIntentRecovery::Moved => {
                        self.intent = None;
                    }
                }
            }
            self.classify_external_release();
        }

        fn classify_external_release(&mut self) {
            match classify_release_movement(
                &self.persisted_release,
                &self.release,
                self.ancestor(self.persisted_release, self.release),
                self.ancestor(self.release, self.staging),
            ) {
                ReleaseMovement::Unchanged => {}
                ReleaseMovement::AdoptUncertified => {
                    self.persisted_release = self.release;
                    self.release_history.push(self.release);
                }
                ReleaseMovement::Block => self.blocked = true,
            }
        }

        fn external_release_fast_forward(&mut self) {
            if self.intent.is_some() || self.release == self.staging {
                return;
            }
            let position = self.position(self.release) + 1;
            self.release = self.chain[position];
            self.classify_external_release();
        }

        fn adopt_external_staging(&mut self) {
            // An external fast-forward of `staging`, adopted by an explicit reconciliation.
            if self.blocked {
                return;
            }
            let old = self.staging;
            self.append();
            let second = self.append();
            self.adoptions.push((old, second));
            self.staging = second;
            if !self.release_stage {
                self.release = second;
                self.persisted_release = second;
                self.release_history.push(second);
            }
        }

        /// The remote gained commits on top of `staging`; `tg pull` moves both refs to it.
        fn pull(&mut self) {
            if self.blocked
                || self
                    .intent
                    .as_ref()
                    .is_some_and(|intent| matches!(intent.state, IntentState::ExternalApplied))
            {
                return;
            }
            if self.remote != Some(self.staging) && self.remote != Some(self.release) {
                return;
            }
            let commit = self.append();
            self.remote = Some(commit);
            self.remote_history.push(commit);
            self.staging = commit;
            self.release = commit;
            self.persisted_release = commit;
            self.release_history.push(commit);
            // The remote now contains every outstanding release promise.
            self.intent = None;
            self.push_blocked = false;
        }

        /// `tg push`: publish `release` when it is certified and its chain is Tollgate's.
        fn tg_push(&mut self) {
            if self.blocked
                || self
                    .intent
                    .as_ref()
                    .is_some_and(|intent| intent.state == IntentState::ExternalApplied)
            {
                return;
            }
            let Some(remote) = self.remote else {
                return;
            };
            let certified = !self.release_stage
                || self.certified.contains(&self.release)
                || self.release == remote;
            if !certified || !self.ancestor(remote, self.release) {
                return;
            }
            if remote != self.release {
                if !release_range_certified(
                    &remote,
                    &self.range(remote, self.release),
                    &self.promotions,
                    &self.adoptions,
                ) {
                    return;
                }
                assert!(!self.release_stage || self.certified.contains(&self.release));
                self.pushes.push((Some(remote), self.release));
                self.remote = Some(self.release);
                self.remote_history.push(self.release);
            }
            self.intent = None;
            self.push_blocked = false;
        }

        fn toggle_stage(&mut self) {
            if self.intent.is_some() {
                return;
            }
            self.release_stage = !self.release_stage;
        }

        fn check_invariants(&self) {
            // R1: `release` is `staging` or a first-parent ancestor of it.
            assert!(self.ancestor(self.release, self.staging), "{self:?}");
            // Persisted and real `release` differ only inside an unfinished intent.
            if self
                .intent
                .as_ref()
                .is_none_or(|intent| intent.state != IntentState::Prepared)
            {
                assert_eq!(self.release, self.persisted_release, "{self:?}");
            }
            // R2: `release` never holds an OID it was not legitimately given, so it never skips
            // past an uncertified tip.
            assert!(self.release_history.contains(&self.release), "{self:?}");
            // R4: every push used an exact lease on the previous remote OID and published a
            // certified OID (or the opt-out promotion's), never moving the remote backwards.
            for (lease, pushed) in &self.pushes {
                let lease = lease.unwrap();
                assert!(self.ancestor(lease, *pushed), "{self:?}");
                assert!(self.release_history.contains(pushed), "{self:?}");
            }
            for pair in self.remote_history.windows(2) {
                assert!(self.ancestor(pair[0], pair[1]), "{self:?}");
            }
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(512))]

        /// R1–R5 under random interleavings of promotions, release passes and failures, advances
        /// that crash at every durable boundary, restarts, pulls, pushes, and external movement.
        #[test]
        fn release_invariants_hold_under_random_interleavings(
            max_release_lag in 0u32..4,
            ops in proptest::collection::vec(op(), 1..60),
        ) {
            let mut world = World::new(max_release_lag);
            for op in ops {
                match op {
                    Op::Promote { release_fix } => world.promote(release_fix),
                    Op::AdoptExternalStaging => world.adopt_external_staging(),
                    Op::ReleaseRun { pass } => world.release_run(pass),
                    Op::Advance { crash, push_fails } => {
                        world.advance(crash, push_fails);
                        if !matches!(crash, Crash::None) {
                            world.restart();
                        }
                    }
                    Op::Restart => world.restart(),
                    Op::Pull => world.pull(),
                    Op::TgPush => world.tg_push(),
                    Op::ExternalReleaseFastForward => world.external_release_fast_forward(),
                    Op::ToggleStage => world.toggle_stage(),
                }
                world.check_invariants();
            }
            // Recovery converges: with no further faults, an advance and a restart leave no
            // prepared intent, and a certified tip with a clear remote is released.
            world.restart();
            world.advance(Crash::None, false);
            world.advance(Crash::None, false);
            world.check_invariants();
            let settled = world.intent.as_ref().is_none_or(|intent| {
                matches!(intent.state, IntentState::NeedsAttention { .. })
            });
            prop_assert!(settled, "unsettled intent after recovery: {:?}", world.intent);
        }

        /// A release-fix candidate always promotes, whatever the lag and the release verdict.
        #[test]
        fn release_fix_candidates_always_promote(
            max_release_lag in 1u32..4,
            lag in 0u64..10,
            failed in any::<bool>(),
        ) {
            prop_assert!(!release_lag_pauses_promotion(max_release_lag, lag, failed, true));
        }
    }
}
