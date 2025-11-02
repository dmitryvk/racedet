use std::num::NonZeroU64;

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
        if let Some(task_id) = task_id
            && let Some(scheduler) = scheduler.as_deref()
        {
            scheduler.on_reached_point(task_id, "start").await;
        }
        let res = TaskFuture::new(inner, task_id).await;
        if let Some(task_id) = task_id
            && let Some(scheduler) = scheduler.as_deref()
        {
            scheduler.on_reached_point(task_id, "end").await;
        }
        res
    }
}

pub async fn run_with_schedule<T, Fut>(inner: Fut) -> (Trace, RunResult<T>)
where
    Fut: Future<Output = T> + Sized,
{
    let scheduler = Scheduler::new();
    let res = executor::run(scheduler.clone(), inner).await;
    let trace = scheduler.get_trace();
    (trace, res)
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
