use std::{num::NonZeroU64, sync::Arc, task::Poll};

use pin_project::pin_project;
use tokio::sync::Barrier;

use crate::{
    executor::TaskFuture,
    scheduler::{RandomTaskSelector, Scheduler},
    sync_model::{SyncEvent, SyncInitEvent},
};
pub mod capture_panics;
mod executor;
mod scheduler;
pub mod sync_model;

#[derive(Clone)]
pub struct RegisteredTaskId(Option<(Arc<Scheduler>, TaskId)>);

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TaskId(NonZeroU64);

#[derive(Clone, Copy, Debug)]
pub struct TaskStartBarrierId(NonZeroU64);

impl RegisteredTaskId {
    const INVALID: RegisteredTaskId = RegisteredTaskId(None);

    fn into_parts(mut self) -> Option<(Arc<Scheduler>, TaskId)> {
        self.0.take()
    }
}

impl Drop for RegisteredTaskId {
    fn drop(&mut self) {
        if let Some((scheduler, task_id)) = self.0.take() {
            scheduler.on_task_finished(task_id);
        }
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

#[derive(Clone)]
pub struct StartBarrier(Arc<Barrier>);

pub fn new_start_barrier(count: usize) -> StartBarrier {
    use sync_model::start_barrier::{BarrierId, NewBarrier};
    let start_barrier = Arc::new(Barrier::new(count));
    sync_init_event(NewBarrier {
        barrier: BarrierId::new(&start_barrier),
        capacity: 2,
    });
    StartBarrier(start_barrier)
}

pub fn register_task(name: &str) -> RegisteredTaskId {
    if let Some(scheduler) = Scheduler::current() {
        let task_id = scheduler.register_task(name, None);
        RegisteredTaskId(Some((scheduler, task_id)))
    } else {
        RegisteredTaskId::INVALID
    }
}

pub async fn with_start_barrier<T>(barrier: StartBarrier, inner: impl Future<Output = T>) -> T {
    use sync_model::start_barrier::{BarrierId, CompletedBarrierWait, WaitingForBarrier};
    tracing::debug!("sync_event barrier waiting");
    sync_event(WaitingForBarrier(BarrierId::new(&barrier.0)));
    execution_point("barrier").await;
    tracing::debug!("barrier waiting");
    barrier.0.wait().await;
    tracing::debug!("barrier wait complete");
    sync_event(CompletedBarrierWait(BarrierId::new(&barrier.0)));
    inner.await
}

pub async fn task<T>(task_id: RegisteredTaskId, inner: impl Future<Output = T>) -> T {
    let _guard;
    let task_id = if let Some((scheduler, task_id)) = task_id.into_parts() {
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

pub fn new_scheduler() -> (SchedulerHandle, impl Future<Output = ()>) {
    let scheduler = SchedulerHandle(Scheduler::new(scheduler::TaskSelector::Random(
        RandomTaskSelector::new(),
    )));
    let control_fut = scheduler.clone().run_control_loop(None);
    (scheduler, control_fut)
}

pub fn new_scheduler_with_start_task_barrier(
    barrier_name: &str,
    num_tasks: usize,
) -> (
    SchedulerHandle,
    TaskStartBarrierId,
    impl Future<Output = ()>,
) {
    let scheduler = SchedulerHandle(Scheduler::new(scheduler::TaskSelector::Random(
        RandomTaskSelector::new(),
    )));
    let start_task_barrier = scheduler.register_task_start_barrier(barrier_name, num_tasks);
    let control_fut = scheduler.clone().run_control_loop(Some(start_task_barrier));
    (scheduler, start_task_barrier, control_fut)
}

pub fn current_scheduler() -> Option<SchedulerHandle> {
    Scheduler::current().map(SchedulerHandle)
}

impl SchedulerHandle {
    pub fn register_task_start_barrier(&self, name: &str, num_tasks: usize) -> TaskStartBarrierId {
        self.0.register_task_start_barrier(name, num_tasks)
    }

    pub fn register_task(
        &self,
        name: &str,
        start_barrier: Option<TaskStartBarrierId>,
    ) -> RegisteredTaskId {
        let task_id = self.0.register_task(name, start_barrier);
        RegisteredTaskId(Some((self.0.clone(), task_id)))
    }

    async fn run_control_loop(self, stop_barrier: Option<TaskStartBarrierId>) {
        self.0.run_control_loop(stop_barrier).await;
    }

    pub fn get_trace(&self) -> Trace {
        self.0.get_trace()
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

#[pin_project]
struct RunAlong<MainFut, AuxFut> {
    #[pin]
    main_fut: MainFut,
    #[pin]
    aux_fut: futures::future::Fuse<AuxFut>,
}

impl<MainFut: Future, AuxFut: Future> Future for RunAlong<MainFut, AuxFut> {
    type Output = MainFut::Output;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let this = self.project();
        if let Poll::Ready(value) = this.main_fut.poll(cx) {
            return Poll::Ready(value);
        }
        _ = this.aux_fut.poll(cx);
        Poll::Pending
    }
}

#[derive(Debug, Clone)]
pub enum RunResult<T> {
    Ok(T),
    Panic(PanicInfo),
}

#[derive(Debug, Clone)]
pub struct Trace {
    pub trace: Vec<String>,
}

impl std::fmt::Display for Trace {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for (i, step) in self.trace.iter().enumerate() {
            writeln!(f, "{i}. {step}")?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct PanicInfo {
    pub message: String,
    pub location: String,
    pub backtrace: String,
}

#[pin_project]
pub struct WithScheduler<Fut> {
    scheduler: Option<Arc<Scheduler>>,
    #[pin]
    inner: Fut,
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
