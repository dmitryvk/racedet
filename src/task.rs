use std::{num::NonZeroU64, sync::Arc};

use futures_executor::block_on;
use tokio::sync::Barrier;

use crate::{
    current_scheduler,
    executor::{self, CurrentTaskIdGuard, TaskFuture},
    scheduler::Scheduler,
    sync_model::{
        SyncEvent, SyncInitEvent,
        start_barrier::{BarrierId, NewBarrier},
    },
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TaskId(NonZeroU64);

impl TaskId {
    pub(crate) fn new(id: NonZeroU64) -> Self {
        Self(id)
    }

    pub(crate) fn get(self) -> NonZeroU64 {
        self.0
    }
}

impl std::fmt::Display for TaskId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

pub fn sync_event<T: SyncEvent>(event: T) {
    if let Some(scheduler) = Scheduler::current()
        && let Some(task_id) = executor::current_task()
        && let Err(err) = scheduler.on_sync_event(task_id, event)
    {
        panic!("invalid sync event: {err}");
    }
}

pub fn sync_init_event<T: SyncInitEvent>(event: T) {
    if let Some(scheduler) = Scheduler::current()
        && let Err(err) = scheduler.on_sync_init_event(event)
    {
        panic!("invalid sync event: {err}");
    }
}

pub async fn execution_point(name: &str) {
    if let Some(scheduler) = Scheduler::current()
        && let Some(task_id) = executor::current_task()
    {
        scheduler.on_reached_point(task_id, name).await;
    }
}

pub async fn execution_point_with_event<T: SyncEvent>(name: &str, event: T) {
    if let Some(scheduler) = Scheduler::current()
        && let Some(task_id) = executor::current_task()
        && let Err(err) = scheduler
            .on_reached_point_with_event(task_id, name, event)
            .await
    {
        panic!("invalid sync event: {err}");
    }
}

pub fn execution_point_blocking(name: &str) {
    block_on(execution_point(name));
}

#[derive(Clone)]
pub struct StartBarrier(Option<Arc<Barrier>>);

pub fn new_start_barrier(task_count: usize) -> StartBarrier {
    if current_scheduler().is_some() {
        let start_barrier = Arc::new(Barrier::new(task_count));
        sync_init_event(NewBarrier {
            barrier: BarrierId::new(&start_barrier),
            capacity: task_count,
        });
        StartBarrier(Some(start_barrier))
    } else {
        StartBarrier(None)
    }
}

pub async fn with_start_barrier<T>(barrier: StartBarrier, inner: impl Future<Output = T>) -> T {
    wait_for_start_barrier(barrier).await;
    inner.await
}

async fn wait_for_start_barrier(barrier: StartBarrier) {
    use crate::sync_model::start_barrier::{BarrierId, CompletedBarrierWait, WaitingForBarrier};
    if let StartBarrier(Some(barrier)) = barrier {
        tracing::debug!("sync_event barrier waiting");
        execution_point_with_event("barrier", WaitingForBarrier(BarrierId::new(&barrier))).await;
        tracing::debug!("barrier waiting");
        barrier.wait().await;
        tracing::debug!("barrier wait complete");
        sync_event(CompletedBarrierWait(BarrierId::new(&barrier)));
    }
}

pub fn with_start_barrier_blocking<T>(barrier: StartBarrier, inner: impl FnOnce() -> T) -> T {
    block_on(wait_for_start_barrier(barrier));
    inner()
}

pub async fn with_task_group<T>(
    task_group: crate::sync_model::task_wait::TaskGroup,
    task_idx: usize,
    inner: impl Future<Output = T>,
) -> T {
    let res = inner.await;
    sync_event(crate::sync_model::task_wait::TaskCompleted(
        task_group, task_idx,
    ));
    res
}

pub fn with_task_group_blocking<T>(
    task_group: crate::sync_model::task_wait::TaskGroup,
    task_idx: usize,
    inner: impl FnOnce() -> T,
) -> T {
    let res = inner();
    sync_event(crate::sync_model::task_wait::TaskCompleted(
        task_group, task_idx,
    ));
    res
}

pub async fn task<T>(name: impl AsRef<str>, inner: impl Future<Output = T>) -> T {
    let _guard;
    let task_id = if let Some(scheduler) = Scheduler::current() {
        let task_id = scheduler.register_task(name.as_ref());
        _guard = TaskFinishedGuard {
            task_id,
            scheduler: scheduler.clone(),
        };
        Some(task_id)
    } else {
        None
    };
    TaskFuture::new(inner, task_id).await
}

pub fn task_blocking<T>(name: impl AsRef<str>, inner: impl FnOnce() -> T) -> T {
    let _guard;
    if let Some(scheduler) = Scheduler::current() {
        let task_id = scheduler.register_task(name.as_ref());
        _guard = (
            TaskFinishedGuard {
                task_id,
                scheduler: scheduler.clone(),
            },
            CurrentTaskIdGuard::install(task_id),
        );
    };
    inner()
}

struct TaskFinishedGuard {
    task_id: TaskId,
    scheduler: Arc<Scheduler>,
}

impl Drop for TaskFinishedGuard {
    fn drop(&mut self) {
        self.scheduler.on_task_finished(self.task_id);
    }
}
