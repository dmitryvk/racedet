use std::{borrow::Cow, cell::RefCell, num::NonZeroU64, sync::Arc, task::Poll};

use futures_executor::block_on;
use futures_util::future::Either;
use pin_project_lite::pin_project;
use tokio::sync::Barrier;

use crate::{
    SchedulerHandle, current_scheduler,
    scheduler::Scheduler,
    sync_model::{
        SyncEvent, SyncInitEvent,
        start_barrier::{BarrierId, NewBarrier},
        task_wait::TaskGroup,
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
        && let Some(task_id) = current_task()
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
        && let Some(task_id) = current_task()
    {
        scheduler.on_reached_point(task_id, name).await;
    }
}

pub async fn execution_point_with_event<T: SyncEvent>(name: &str, event: T) {
    if let Some(scheduler) = Scheduler::current()
        && let Some(task_id) = current_task()
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

impl StartBarrier {
    pub fn new(task_count: usize) -> Self {
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

pub struct Task {
    name: Cow<'static, str>,
    scheduler: Option<SchedulerHandle>,
    task_group: Option<(TaskGroup, usize)>,
    start_barrier: Option<StartBarrier>,
}

impl Task {
    pub fn new(name: impl Into<Cow<'static, str>>) -> Task {
        Self {
            name: name.into(),
            scheduler: current_scheduler(),
            task_group: None,
            start_barrier: None,
        }
    }

    pub fn with_scheduler(mut self, scheduler: Option<SchedulerHandle>) -> Self {
        self.scheduler = scheduler;
        self
    }

    pub fn with_start_barrier(mut self, start_barrier: StartBarrier) -> Self {
        self.start_barrier = Some(start_barrier);
        self
    }

    pub fn with_task_group(mut self, task_group: TaskGroup, task_idx: usize) -> Self {
        self.task_group = Some((task_group, task_idx));
        self
    }
}

pub fn task<T>(task: Task, inner: impl Future<Output = T>) -> impl Future<Output = T> {
    let Some(scheduler) = task.scheduler else {
        return Either::Left(inner);
    };

    Either::Right(TaskFuture::new(
        async move {
            if let Some((task_group, task_idx)) = task.task_group {
                sync_event(crate::sync_model::task_wait::TaskStarted(
                    task_group, task_idx,
                ));
            }
            if let Some(barrier) = task.start_barrier {
                wait_for_start_barrier(barrier).await;
            }
            let res = inner.await;
            if let Some((task_group, task_idx)) = task.task_group {
                sync_event(crate::sync_model::task_wait::TaskCompleted(
                    task_group, task_idx,
                ));
            }
            res
        },
        task.name,
        scheduler.0,
    ))
}

pub fn task_blocking<T>(task: Task, inner: impl FnOnce() -> T) -> T {
    let Some(scheduler) = task.scheduler else {
        return inner();
    };

    let task_id = scheduler.0.register_task(task.name.as_ref());
    let _guard = (
        TaskFinishedGuard {
            task_id,
            scheduler: scheduler.0.clone(),
        },
        scheduler.0.set_current(),
        CurrentTaskIdGuard::install(task_id),
    );

    if let Some((task_group, task_idx)) = task.task_group {
        sync_event(crate::sync_model::task_wait::TaskStarted(
            task_group, task_idx,
        ));
    }

    if let Some(barrier) = task.start_barrier {
        block_on(wait_for_start_barrier(barrier));
    }
    let res = inner();

    if let Some((task_group, task_idx)) = task.task_group {
        sync_event(crate::sync_model::task_wait::TaskCompleted(
            task_group, task_idx,
        ));
    }
    res
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

thread_local! {
    static CURRENT_TASK: RefCell<Option<TaskId>> = const { RefCell::new(None) };
}

pin_project! {
    pub(crate) struct TaskFuture<Fut> {
        #[pin]
        inner: Fut,
        task_name: Cow<'static, str>,
        task_id: Option<TaskId>,
        scheduler: Arc<Scheduler>,
        finished_guard: Option<TaskFinishedGuard>,
    }
}

impl<Fut> TaskFuture<Fut> {
    pub(crate) fn new(inner: Fut, task_name: Cow<'static, str>, scheduler: Arc<Scheduler>) -> Self {
        Self {
            inner,
            task_name,
            task_id: None,
            scheduler,
            finished_guard: None,
        }
    }
}

impl<Fut: Future> Future for TaskFuture<Fut> {
    type Output = Fut::Output;

    fn poll(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        let task_id = if let Some(task_id) = this.task_id {
            *task_id
        } else {
            let task_id = this.scheduler.register_task(&this.task_name);
            *this.task_id = Some(task_id);
            *this.finished_guard = Some(TaskFinishedGuard {
                task_id,
                scheduler: this.scheduler.clone(),
            });
            task_id
        };
        let _task_id_guard = CurrentTaskIdGuard::install(task_id);
        let _scheduler_guard = this.scheduler.set_current();

        this.inner.poll(cx)
    }
}

pub(crate) fn current_task() -> Option<TaskId> {
    CURRENT_TASK.with_borrow(|t| *t)
}

pub(crate) struct CurrentTaskIdGuard {
    old_value: Option<TaskId>,
}

impl CurrentTaskIdGuard {
    pub(crate) fn install(task_id: TaskId) -> Self {
        let old_value = CURRENT_TASK.replace(Some(task_id));

        Self { old_value }
    }
}

impl Drop for CurrentTaskIdGuard {
    fn drop(&mut self) {
        CURRENT_TASK.with_borrow_mut(|v| *v = self.old_value);
    }
}
