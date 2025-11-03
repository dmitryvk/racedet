use std::{num::NonZeroU64, sync::Arc, task::Poll};

use futures::FutureExt;
use pin_project::pin_project;

use crate::{executor::TaskFuture, scheduler::Scheduler};
mod executor;
mod scheduler;

#[derive(Clone)]
pub struct RegisteredTaskId(Option<(Arc<Scheduler>, TaskId)>);

#[derive(Clone, Copy, Debug)]
struct TaskId(NonZeroU64);

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

pub async fn execution_point(name: &str) {
    if let Some(scheduler) = Scheduler::current()
        && let Some(task_id) = executor::current_task()
    {
        scheduler.on_reached_point(task_id, name).await;
    }
}

pub fn register_task(name: &str) -> RegisteredTaskId {
    if let Some(scheduler) = Scheduler::current() {
        let task_id = scheduler.register_task(name);
        RegisteredTaskId(Some((scheduler, task_id)))
    } else {
        RegisteredTaskId::INVALID
    }
}

pub async fn task<T>(task_id: RegisteredTaskId, inner: impl Future<Output = T>) -> T {
    let _guard;
    let task_id = if let Some((scheduler, task_id)) = task_id.into_parts() {
        _guard = TaskFinishedGuard {
            task_id,
            scheduler: scheduler.clone(),
        };
        scheduler.on_task_started(task_id).await;
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

pub async fn run_with_schedule<T, Fut>(inner: Fut) -> (Trace, RunResult<T>)
where
    Fut: Future<Output = T> + Sized,
{
    let scheduler = Scheduler::new();
    let res = RunAlong {
        main_fut: executor::run(scheduler.clone(), inner),
        aux_fut: scheduler.run_control_loop().fuse(),
    }
    .await;
    let trace = scheduler.get_trace();
    (trace, res)
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
