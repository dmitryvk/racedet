use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use tokio::sync::Barrier;

use crate::{
    sync::{
        BadSync, DynSyncModel, NotificationOutcome, ProcessSyncEvent, ProcessSyncInitEvent,
        SyncEvent, SyncInitEvent, SyncModel, TaskProgressDependencies,
    },
    task::TaskId,
};

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
pub struct BarrierId(usize);

impl BarrierId {
    pub fn new(barrier: &Arc<Barrier>) -> Self {
        let ptr = Arc::as_ptr(barrier);
        BarrierId(ptr as usize)
    }
}

#[derive(Default, Debug)]
pub struct BarrierModel {
    task_waiting: HashMap<TaskId, BarrierId>,
    barriers: HashMap<BarrierId, BarrierState>,
}

#[derive(Debug)]
struct BarrierState {
    capacity: usize,
    reached: bool,
    tasks: HashSet<TaskId>,
}

impl SyncModel for BarrierModel {}

impl DynSyncModel for BarrierModel {
    fn task_progress_dependencies(&self, task_id: TaskId) -> TaskProgressDependencies {
        let Some(barrier_id) = self.task_waiting.get(&task_id) else {
            return TaskProgressDependencies::Ready {
                need_to_run: HashSet::new(),
            };
        };
        let Some(barrier) = self.barriers.get(barrier_id) else {
            return TaskProgressDependencies::Ready {
                need_to_run: HashSet::new(),
            };
        };
        if barrier.reached {
            TaskProgressDependencies::Ready {
                need_to_run: barrier.tasks.clone(),
            }
        } else {
            TaskProgressDependencies::Blocked
        }
    }
}

pub struct NewBarrier {
    pub barrier: BarrierId,
    pub capacity: usize,
}
impl SyncInitEvent for NewBarrier {
    type Model = BarrierModel;
}

pub struct WaitingForBarrier(pub BarrierId);
impl SyncEvent for WaitingForBarrier {
    type Model = BarrierModel;
}

pub struct AbortedWaitingForBarrier(pub BarrierId);
impl SyncEvent for AbortedWaitingForBarrier {
    type Model = BarrierModel;
}

pub struct CompletedBarrierWait(pub BarrierId);
impl SyncEvent for CompletedBarrierWait {
    type Model = BarrierModel;
}

impl ProcessSyncInitEvent<NewBarrier> for BarrierModel {
    fn on_init_event(&mut self, event: NewBarrier) -> Result<NotificationOutcome, BadSync> {
        self.barriers.insert(
            event.barrier,
            BarrierState {
                capacity: event.capacity,
                reached: false,
                tasks: HashSet::new(),
            },
        );
        Ok(NotificationOutcome::Acknowledged)
    }
}

impl ProcessSyncEvent<WaitingForBarrier> for BarrierModel {
    fn on_event(
        &mut self,
        task_id: TaskId,
        WaitingForBarrier(barrier_id): WaitingForBarrier,
    ) -> Result<NotificationOutcome, BadSync> {
        tracing::debug!("barriers before waiting {barrier_id:?}: {self:?}");
        let barrier = self
            .barriers
            .get_mut(&barrier_id)
            .ok_or_else(|| BadSync("barrier not exists".to_string()))?;
        if self.task_waiting.insert(task_id, barrier_id).is_some() {
            return Err(BadSync("task is already waiting on a barrier".to_string()));
        }
        barrier.tasks.insert(task_id);
        if !barrier.reached && barrier.tasks.len() >= barrier.capacity {
            barrier.reached = true;
        }
        tracing::debug!("barriers after waiting {barrier_id:?}: {self:?}");
        Ok(NotificationOutcome::Acknowledged)
    }
}

impl ProcessSyncEvent<AbortedWaitingForBarrier> for BarrierModel {
    fn on_event(
        &mut self,
        task_id: TaskId,
        AbortedWaitingForBarrier(barrier_id): AbortedWaitingForBarrier,
    ) -> Result<NotificationOutcome, BadSync> {
        let barrier = self
            .barriers
            .get_mut(&barrier_id)
            .ok_or_else(|| BadSync("barrier not exists".to_string()))?;
        barrier.tasks.remove(&task_id);
        if barrier.tasks.is_empty() {
            self.barriers.remove(&barrier_id);
        }
        self.task_waiting.remove(&task_id);
        Ok(NotificationOutcome::Acknowledged)
    }
}

impl ProcessSyncEvent<CompletedBarrierWait> for BarrierModel {
    fn on_event(
        &mut self,
        task_id: TaskId,
        CompletedBarrierWait(barrier_id): CompletedBarrierWait,
    ) -> Result<NotificationOutcome, BadSync> {
        let barrier = self
            .barriers
            .get_mut(&barrier_id)
            .ok_or_else(|| BadSync("barrier not exists".to_string()))?;
        barrier.tasks.remove(&task_id);
        if barrier.tasks.is_empty() {
            self.barriers.remove(&barrier_id);
        }
        self.task_waiting.remove(&task_id);
        Ok(NotificationOutcome::Acknowledged)
    }
}
