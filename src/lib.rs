use std::{num::NonZeroU64, task::Poll};

use futures::FutureExt;
use pin_project::pin_project;

use crate::{executor::TaskFuture, scheduler::Scheduler};
mod executor;
mod scheduler;

#[derive(Clone, Copy, Debug)]
struct TaskId(NonZeroU64);

pub async fn execution_point(name: &str) {
    if let Some(scheduler) = Scheduler::current()
        && let Some(task_id) = executor::current_task()
    {
        scheduler.on_reached_point(task_id, name).await;
    }
}

pub fn task<T>(name: &str, inner: impl Future<Output = T>) -> impl Future<Output = T> {
    let scheduler = Scheduler::current();
    let task_id = scheduler.as_deref().map(|s| {
        let task_id = s.register_task(name);
        println!("task {task_id:?} {name}");
        task_id
    });
    async move {
        let _guard;
        if let Some(task_id) = task_id
            && let Some(scheduler) = scheduler.as_deref()
        {
            scheduler.on_task_started(task_id);
            _guard = TaskFinishedGuard { task_id, scheduler };
        }
        TaskFuture::new(inner, task_id).await
    }
}

struct TaskFinishedGuard<'a> {
    task_id: TaskId,
    scheduler: &'a Scheduler,
}

impl Drop for TaskFinishedGuard<'_> {
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
