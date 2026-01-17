use std::{num::NonZeroU64, sync::Arc, task::Poll};

use futures_executor::block_on;
use pin_project_lite::pin_project;
use tokio::sync::Barrier;

use crate::{
    executor::{CurrentTaskIdGuard, TaskFuture},
    full_trace::Trace,
    scheduler::{RandomTaskSelector, Scheduler},
    sync_model::{SyncEvent, SyncInitEvent},
};
pub mod driver;
mod executor;
pub mod full_trace;
mod replay_trace;
mod replay_trace_parsed;
mod scheduler;
mod string_pool;
pub mod sync_model;

pub use replay_trace_parsed::{ParseError as ReplayTraceParseError, ReplayTrace};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TaskId(NonZeroU64);

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct TaskStableId(NonZeroU64);

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
    if let Some(scheduler) = current_scheduler() {
        scheduler.new_start_barrier(task_count)
    } else {
        StartBarrier(None)
    }
}

pub async fn with_start_barrier<T>(barrier: StartBarrier, inner: impl Future<Output = T>) -> T {
    wait_for_start_barrier(barrier).await;
    inner.await
}

async fn wait_for_start_barrier(barrier: StartBarrier) {
    use sync_model::start_barrier::{BarrierId, CompletedBarrierWait, WaitingForBarrier};
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
    task_group: sync_model::task_wait::TaskGroup,
    task_idx: usize,
    inner: impl Future<Output = T>,
) -> T {
    let res = inner.await;
    sync_event(sync_model::task_wait::TaskCompleted(task_group, task_idx));
    res
}

pub fn with_task_group_blocking<T>(
    task_group: sync_model::task_wait::TaskGroup,
    task_idx: usize,
    inner: impl FnOnce() -> T,
) -> T {
    let res = inner();
    sync_event(sync_model::task_wait::TaskCompleted(task_group, task_idx));
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

#[derive(Clone)]
pub struct SchedulerHandle(Arc<Scheduler>);

pub fn new_scheduler(
    replay: Option<&ReplayTrace>,
) -> (SchedulerHandle, impl Future<Output = ()> + use<>) {
    let scheduler = SchedulerHandle(Scheduler::new(
        replay,
        scheduler::TaskSelector::Random(RandomTaskSelector::new()),
    ));
    let control_fut = scheduler.clone().run_control_loop();
    (scheduler, control_fut)
}

pub fn current_scheduler() -> Option<SchedulerHandle> {
    Scheduler::current().map(SchedulerHandle)
}

impl SchedulerHandle {
    async fn run_control_loop(self) {
        self.0.run_control_loop().await;
    }

    pub fn get_trace(&self) -> Trace {
        self.0.get_trace()
    }

    pub fn get_replay(&self) -> ReplayTrace {
        crate::replay_trace::ReplayTrace::from_trace(self.0.string_pool(), &self.0.get_trace())
            .to_parsed()
    }

    pub fn new_start_barrier(&self, task_count: usize) -> StartBarrier {
        use sync_model::start_barrier::{BarrierId, NewBarrier};
        let start_barrier = Arc::new(Barrier::new(task_count));
        self.0
            .on_sync_init_event(NewBarrier {
                barrier: BarrierId::new(&start_barrier),
                capacity: task_count,
            })
            .expect("should register succesfully");
        StartBarrier(Some(start_barrier))
    }
}

pub fn with_scheduler<Fut>(scheduler: SchedulerHandle, inner: Fut) -> WithScheduler<Fut> {
    WithScheduler {
        scheduler: Some(scheduler.0),
        inner,
    }
}

pub fn maybe_with_scheduler<Fut>(
    scheduler: Option<SchedulerHandle>,
    inner: Fut,
) -> WithScheduler<Fut> {
    WithScheduler {
        scheduler: scheduler.map(|handle| handle.0),
        inner,
    }
}

pub fn maybe_with_scheduler_blocking<T>(
    scheduler: Option<SchedulerHandle>,
    inner: impl FnOnce() -> T,
) -> T {
    let _guard;
    if let Some(scheduler) = scheduler {
        _guard = scheduler.0.set_current();
    }
    inner()
}

pin_project! {
    pub struct WithScheduler<Fut> {
        scheduler: Option<Arc<Scheduler>>,
        #[pin]
        inner: Fut,
    }
}

impl<Fut: Future> Future for WithScheduler<Fut> {
    type Output = Fut::Output;

    fn poll(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        let _guard;
        if let Some(scheduler) = &this.scheduler {
            _guard = scheduler.set_current();
        }
        this.inner.poll(cx)
    }
}

impl std::fmt::Display for TaskId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl std::fmt::Display for TaskStableId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
