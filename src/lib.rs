use std::{num::NonZeroU64, sync::Arc, task::Poll};

use pin_project_lite::pin_project;

use crate::{
    full_trace::TraceView,
    scheduler::{RandomTaskSelector, Scheduler},
};
pub mod driver;
pub mod full_trace;
mod replay_trace;
mod replay_trace_parsed;
mod scheduler;
mod string_pool;
pub mod sync_model;
pub mod task;

pub use replay_trace_parsed::{ParseError as ReplayTraceParseError, ReplayTrace};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct TaskStableId(NonZeroU64);

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

    pub fn get_trace(&self) -> TraceView {
        self.0.get_trace()
    }

    pub fn get_replay(&self) -> ReplayTrace {
        crate::replay_trace::ReplayTrace::from_trace(self.0.string_pool(), &self.0.get_trace())
            .to_parsed()
    }
}

pub fn with_scheduler<Fut>(scheduler: SchedulerHandle, inner: Fut) -> WithScheduler<Fut> {
    WithScheduler {
        scheduler: Some(scheduler.0),
        inner,
    }
}

pub fn with_scheduler_opt<Fut>(
    scheduler: Option<SchedulerHandle>,
    inner: Fut,
) -> WithScheduler<Fut> {
    WithScheduler {
        scheduler: scheduler.map(|handle| handle.0),
        inner,
    }
}

pub fn with_scheduler_blocking<T>(scheduler: &SchedulerHandle, inner: impl FnOnce() -> T) -> T {
    let _guard = scheduler.0.set_current();
    inner()
}

pub fn with_scheduler_blocking_opt<T>(
    scheduler: Option<&SchedulerHandle>,
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

impl std::fmt::Display for TaskStableId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
