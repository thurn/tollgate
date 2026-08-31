#![forbid(unsafe_code)]

use std::{
    collections::{BTreeMap, HashMap, HashSet, VecDeque},
    sync::Arc,
};

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
use tollgate_domain::{BuildsetId, RepositoryId, StepId};

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum PriorityClass {
    GateHead = 0,
    Speculative = 1,
    Independent = 2,
    Maintenance = 3,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct ResourceCapacity {
    pub max_buildsets: u16,
    pub cpu_tokens: u16,
    pub memory_bytes: u64,
    pub semaphores: BTreeMap<String, u16>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct StepResources {
    pub cpu_tokens: u16,
    pub memory_bytes: u64,
    pub semaphores: BTreeMap<String, u16>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DispatchRequest {
    pub repository_id: RepositoryId,
    pub buildset_id: BuildsetId,
    pub priority: PriorityClass,
    pub queue_position: u16,
    pub repository_weight: u16,
    pub affinity_score: i64,
}

#[derive(Debug, Error, Eq, PartialEq)]
pub enum SchedulerError {
    #[error("resource request can never fit in the configured pool: {0}")]
    Unsatisfiable(String),
    #[error("allocation does not exist")]
    UnknownAllocation,
    #[error("resource admission was canceled")]
    Canceled,
}

#[derive(Default)]
struct SchedulerState {
    capacity: ResourceCapacity,
    admitted_buildsets: u16,
    cpu_reserved: u16,
    memory_reserved: u64,
    semaphore_reserved: BTreeMap<String, u16>,
    allocations: HashMap<StepId, StepResources>,
    pending: VecDeque<DispatchRequest>,
    granted: HashSet<BuildsetId>,
    repository_service: HashMap<RepositoryId, u64>,
}

pub struct GlobalScheduler {
    state: Mutex<SchedulerState>,
    changed: Notify,
}

impl GlobalScheduler {
    pub fn new(capacity: ResourceCapacity) -> Self {
        Self {
            state: Mutex::new(SchedulerState {
                capacity,
                ..SchedulerState::default()
            }),
            changed: Notify::new(),
        }
    }

    /// Changes future admission without forgetting live allocations. Shrinking a pool below its
    /// current use is safe: existing work drains and no new request is granted until it fits.
    pub fn reconfigure(&self, capacity: ResourceCapacity) {
        self.state.lock().capacity = capacity;
        self.changed.notify_waiters();
    }

    pub fn capacity(&self) -> ResourceCapacity {
        self.state.lock().capacity.clone()
    }

    pub fn enqueue(&self, request: DispatchRequest) {
        self.state.lock().pending.push_back(request);
    }

    pub fn next_buildset(&self) -> Option<DispatchRequest> {
        let mut state = self.state.lock();
        if state.admitted_buildsets >= state.capacity.max_buildsets || state.pending.is_empty() {
            return None;
        }
        let best = state
            .pending
            .iter()
            .enumerate()
            .min_by_key(|(_, request)| {
                let service = state
                    .repository_service
                    .get(&request.repository_id)
                    .copied()
                    .unwrap_or(0);
                (
                    request.priority,
                    service / u64::from(request.repository_weight.max(1)),
                    request.queue_position,
                    -request.affinity_score,
                )
            })
            .map(|(index, _)| index)?;
        let selected = state.pending.remove(best)?;
        *state
            .repository_service
            .entry(selected.repository_id)
            .or_default() += 1;
        state.admitted_buildsets += 1;
        Some(selected)
    }

    pub async fn acquire_buildset(
        self: &Arc<Self>,
        request: DispatchRequest,
        cancellation: &CancellationToken,
    ) -> Result<BuildsetAllocation, SchedulerError> {
        if cancellation.is_cancelled() {
            return Err(SchedulerError::Canceled);
        }
        let buildset_id = request.buildset_id;
        {
            let mut state = self.state.lock();
            if !state
                .pending
                .iter()
                .any(|entry| entry.buildset_id == buildset_id)
                && !state.granted.contains(&buildset_id)
            {
                state.pending.push_back(request);
            }
        }
        self.changed.notify_waiters();
        loop {
            if cancellation.is_cancelled() {
                let mut state = self.state.lock();
                state
                    .pending
                    .retain(|entry| entry.buildset_id != buildset_id);
                if state.granted.remove(&buildset_id) {
                    state.admitted_buildsets = state.admitted_buildsets.saturating_sub(1);
                }
                drop(state);
                self.changed.notify_waiters();
                return Err(SchedulerError::Canceled);
            }
            let notified = self.changed.notified();
            {
                let mut state = self.state.lock();
                while state.admitted_buildsets < state.capacity.max_buildsets
                    && !state.pending.is_empty()
                {
                    let best = state
                        .pending
                        .iter()
                        .enumerate()
                        .min_by_key(|(_, request)| {
                            let service = state
                                .repository_service
                                .get(&request.repository_id)
                                .copied()
                                .unwrap_or(0);
                            (
                                request.priority,
                                service / u64::from(request.repository_weight.max(1)),
                                request.queue_position,
                                -request.affinity_score,
                            )
                        })
                        .map(|(index, _)| index)
                        .expect("nonempty pending queue has a best request");
                    let selected = state
                        .pending
                        .remove(best)
                        .expect("selected pending request still exists");
                    *state
                        .repository_service
                        .entry(selected.repository_id)
                        .or_default() += 1;
                    state.admitted_buildsets += 1;
                    state.granted.insert(selected.buildset_id);
                }
                if state.granted.remove(&buildset_id) {
                    drop(state);
                    self.changed.notify_waiters();
                    return Ok(BuildsetAllocation {
                        scheduler: Arc::clone(self),
                        active: true,
                    });
                }
            }
            tokio::select! {
                _ = notified => {},
                _ = cancellation.cancelled() => {
                    let mut state = self.state.lock();
                    state.pending.retain(|entry| entry.buildset_id != buildset_id);
                    if state.granted.remove(&buildset_id) {
                        state.admitted_buildsets = state.admitted_buildsets.saturating_sub(1);
                    }
                    drop(state);
                    self.changed.notify_waiters();
                    return Err(SchedulerError::Canceled);
                },
            }
        }
    }

    pub fn release_buildset(&self) {
        let mut state = self.state.lock();
        state.admitted_buildsets = state.admitted_buildsets.saturating_sub(1);
        self.changed.notify_waiters();
    }

    pub fn try_acquire_step(
        &self,
        step_id: StepId,
        request: StepResources,
    ) -> Result<bool, SchedulerError> {
        let mut state = self.state.lock();
        validate_request(&state.capacity, &request)?;
        if state.allocations.contains_key(&step_id) {
            return Ok(true);
        }
        let fits = state.cpu_reserved.saturating_add(request.cpu_tokens)
            <= state.capacity.cpu_tokens
            && state.memory_reserved.saturating_add(request.memory_bytes)
                <= state.capacity.memory_bytes
            && request.semaphores.iter().all(|(name, amount)| {
                state
                    .semaphore_reserved
                    .get(name)
                    .copied()
                    .unwrap_or(0)
                    .saturating_add(*amount)
                    <= state.capacity.semaphores.get(name).copied().unwrap_or(1)
            });
        if !fits {
            return Ok(false);
        }
        state.cpu_reserved += request.cpu_tokens;
        state.memory_reserved += request.memory_bytes;
        for (name, amount) in &request.semaphores {
            *state.semaphore_reserved.entry(name.clone()).or_default() += amount;
        }
        state.allocations.insert(step_id, request);
        Ok(true)
    }

    pub async fn acquire_step(
        self: &Arc<Self>,
        step_id: StepId,
        request: StepResources,
        cancellation: &CancellationToken,
    ) -> Result<StepAllocation, SchedulerError> {
        loop {
            if cancellation.is_cancelled() {
                return Err(SchedulerError::Canceled);
            }
            let notified = self.changed.notified();
            if self.try_acquire_step(step_id, request.clone())? {
                return Ok(StepAllocation {
                    scheduler: Arc::clone(self),
                    step_id: Some(step_id),
                });
            }
            tokio::select! {
                _ = notified => {},
                _ = cancellation.cancelled() => return Err(SchedulerError::Canceled),
            }
        }
    }

    pub fn release_step(&self, step_id: StepId) -> Result<(), SchedulerError> {
        let mut state = self.state.lock();
        let request = state
            .allocations
            .remove(&step_id)
            .ok_or(SchedulerError::UnknownAllocation)?;
        state.cpu_reserved -= request.cpu_tokens;
        state.memory_reserved -= request.memory_bytes;
        for (name, amount) in request.semaphores {
            if let Some(reserved) = state.semaphore_reserved.get_mut(&name) {
                *reserved -= amount;
            }
        }
        drop(state);
        self.changed.notify_waiters();
        Ok(())
    }

    pub fn usage(&self) -> ResourceUsage {
        let state = self.state.lock();
        ResourceUsage {
            admitted_buildsets: state.admitted_buildsets,
            cpu_reserved: state.cpu_reserved,
            memory_reserved: state.memory_reserved,
            semaphore_reserved: state.semaphore_reserved.clone(),
            pending_buildsets: state.pending.len() as u32,
        }
    }
}

fn validate_request(
    capacity: &ResourceCapacity,
    request: &StepResources,
) -> Result<(), SchedulerError> {
    if request.cpu_tokens > capacity.cpu_tokens {
        return Err(SchedulerError::Unsatisfiable("CPU tokens".into()));
    }
    if request.memory_bytes > capacity.memory_bytes {
        return Err(SchedulerError::Unsatisfiable("memory".into()));
    }
    for (name, amount) in &request.semaphores {
        if *amount > capacity.semaphores.get(name).copied().unwrap_or(1) {
            return Err(SchedulerError::Unsatisfiable(format!("semaphore `{name}`")));
        }
    }
    Ok(())
}

pub struct BuildsetAllocation {
    scheduler: Arc<GlobalScheduler>,
    active: bool,
}

impl Drop for BuildsetAllocation {
    fn drop(&mut self) {
        if self.active {
            self.scheduler.release_buildset();
            self.active = false;
        }
    }
}

pub struct StepAllocation {
    scheduler: Arc<GlobalScheduler>,
    step_id: Option<StepId>,
}

impl Drop for StepAllocation {
    fn drop(&mut self) {
        if let Some(step_id) = self.step_id.take() {
            let _ = self.scheduler.release_step(step_id);
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ResourceUsage {
    pub admitted_buildsets: u16,
    pub cpu_reserved: u16,
    pub memory_reserved: u64,
    pub semaphore_reserved: BTreeMap<String, u16>,
    pub pending_buildsets: u32,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scheduler() -> GlobalScheduler {
        GlobalScheduler::new(ResourceCapacity {
            max_buildsets: 2,
            cpu_tokens: 4,
            memory_bytes: 100,
            semaphores: BTreeMap::from([("unity".into(), 1)]),
        })
    }

    #[test]
    fn resources_are_acquired_atomically() {
        let scheduler = scheduler();
        let first = StepId::new();
        assert!(
            scheduler
                .try_acquire_step(
                    first,
                    StepResources {
                        cpu_tokens: 3,
                        memory_bytes: 80,
                        semaphores: BTreeMap::from([("unity".into(), 1)])
                    }
                )
                .unwrap()
        );
        assert!(
            !scheduler
                .try_acquire_step(
                    StepId::new(),
                    StepResources {
                        cpu_tokens: 2,
                        memory_bytes: 10,
                        semaphores: BTreeMap::new()
                    }
                )
                .unwrap()
        );
        assert_eq!(scheduler.usage().cpu_reserved, 3);
        scheduler.release_step(first).unwrap();
        assert_eq!(scheduler.usage().cpu_reserved, 0);
    }

    #[test]
    fn gate_head_wins_over_warm_maintenance() {
        let scheduler = scheduler();
        let repo = RepositoryId::new();
        scheduler.enqueue(DispatchRequest {
            repository_id: repo,
            buildset_id: BuildsetId::new(),
            priority: PriorityClass::Maintenance,
            queue_position: 0,
            repository_weight: 1,
            affinity_score: 100,
        });
        let head = BuildsetId::new();
        scheduler.enqueue(DispatchRequest {
            repository_id: repo,
            buildset_id: head,
            priority: PriorityClass::GateHead,
            queue_position: 0,
            repository_weight: 1,
            affinity_score: 0,
        });
        assert_eq!(scheduler.next_buildset().unwrap().buildset_id, head);
    }

    #[tokio::test]
    async fn cancellation_removes_a_waiter_without_leaking_capacity() {
        let scheduler = Arc::new(GlobalScheduler::new(ResourceCapacity {
            max_buildsets: 1,
            ..ResourceCapacity::default()
        }));
        let repository_id = RepositoryId::new();
        let request = |buildset_id| DispatchRequest {
            repository_id,
            buildset_id,
            priority: PriorityClass::Speculative,
            queue_position: 0,
            repository_weight: 1,
            affinity_score: 0,
        };
        let first_cancel = CancellationToken::new();
        let first = scheduler
            .acquire_buildset(request(BuildsetId::new()), &first_cancel)
            .await
            .unwrap();
        let waiting_cancel = CancellationToken::new();
        let waiting = {
            let scheduler = Arc::clone(&scheduler);
            let request = request(BuildsetId::new());
            let cancellation = waiting_cancel.clone();
            tokio::spawn(async move { scheduler.acquire_buildset(request, &cancellation).await })
        };
        tokio::task::yield_now().await;
        waiting_cancel.cancel();
        assert!(matches!(
            waiting.await.unwrap(),
            Err(SchedulerError::Canceled)
        ));
        assert_eq!(scheduler.usage().pending_buildsets, 0);
        drop(first);
        assert_eq!(scheduler.usage().admitted_buildsets, 0);
    }

    #[tokio::test]
    async fn cancellation_before_enqueue_never_acquires_buildset_or_step_capacity() {
        let scheduler = Arc::new(scheduler());
        let cancellation = CancellationToken::new();
        cancellation.cancel();
        let buildset = scheduler
            .acquire_buildset(
                DispatchRequest {
                    repository_id: RepositoryId::new(),
                    buildset_id: BuildsetId::new(),
                    priority: PriorityClass::Speculative,
                    queue_position: 0,
                    repository_weight: 1,
                    affinity_score: 0,
                },
                &cancellation,
            )
            .await;
        assert!(matches!(buildset, Err(SchedulerError::Canceled)));
        let step = scheduler
            .acquire_step(
                StepId::new(),
                StepResources {
                    cpu_tokens: 1,
                    memory_bytes: 1,
                    semaphores: BTreeMap::from([("unity".into(), 1)]),
                },
                &cancellation,
            )
            .await;
        assert!(matches!(step, Err(SchedulerError::Canceled)));
        assert_eq!(
            scheduler.usage(),
            ResourceUsage {
                admitted_buildsets: 0,
                cpu_reserved: 0,
                memory_reserved: 0,
                semaphore_reserved: BTreeMap::new(),
                pending_buildsets: 0,
            }
        );
    }

    #[tokio::test]
    async fn cancellation_immediately_after_admission_releases_every_resource() {
        let scheduler = Arc::new(scheduler());
        let cancellation = CancellationToken::new();
        let allocation = scheduler
            .acquire_step(
                StepId::new(),
                StepResources {
                    cpu_tokens: 2,
                    memory_bytes: 40,
                    semaphores: BTreeMap::from([("unity".into(), 1)]),
                },
                &cancellation,
            )
            .await
            .unwrap();
        cancellation.cancel();
        drop(allocation);
        let usage = scheduler.usage();
        assert_eq!(usage.cpu_reserved, 0);
        assert_eq!(usage.memory_reserved, 0);
        assert_eq!(usage.semaphore_reserved.get("unity"), Some(&0));
    }

    #[tokio::test]
    async fn shrinking_capacity_preserves_live_allocations_and_blocks_new_work() {
        let scheduler = Arc::new(scheduler());
        let first = StepId::new();
        let second = StepId::new();
        assert!(
            scheduler
                .try_acquire_step(
                    first,
                    StepResources {
                        cpu_tokens: 2,
                        memory_bytes: 40,
                        semaphores: BTreeMap::new(),
                    },
                )
                .unwrap()
        );
        scheduler.reconfigure(ResourceCapacity {
            max_buildsets: 1,
            cpu_tokens: 1,
            memory_bytes: 20,
            semaphores: BTreeMap::new(),
        });
        assert_eq!(scheduler.usage().cpu_reserved, 2);
        assert!(matches!(
            scheduler.try_acquire_step(
                second,
                StepResources {
                    cpu_tokens: 1,
                    memory_bytes: 1,
                    semaphores: BTreeMap::new(),
                }
            ),
            Ok(false)
        ));
        scheduler.release_step(first).unwrap();
        assert!(
            scheduler
                .try_acquire_step(
                    second,
                    StepResources {
                        cpu_tokens: 1,
                        memory_bytes: 1,
                        semaphores: BTreeMap::new(),
                    },
                )
                .unwrap()
        );
    }
}
