use std::{cell::RefCell, panic::AssertUnwindSafe, sync::Arc, task::Poll};

use futures::pin_mut;
use pin_project::pin_project;

use crate::{PanicInfo, RunResult, TaskId, scheduler::Scheduler};

thread_local! {
    static CURRENT_TASK: RefCell<Option<TaskId>> = RefCell::new(None);
}

pub(crate) async fn run<T, Fut>(scheduler: Arc<Scheduler>, fut: Fut) -> RunResult<T>
where
    Fut: Future<Output = T> + Sized,
{
    let panic_fut = MainFut::new(fut, scheduler);
    pin_mut!(panic_fut);
    match panic_fut.await {
        Ok(res) => RunResult::Ok(res),
        Err(panic) => RunResult::Panic(panic),
    }
}

#[pin_project]
struct MainFut<Fut> {
    #[pin]
    inner: Fut,
    scheduler: Arc<Scheduler>,
}

impl<Fut> MainFut<Fut> {
    fn new(inner: Fut, scheduler: Arc<Scheduler>) -> Self {
        Self { inner, scheduler }
    }
}

impl<Fut: Future> Future for MainFut<Fut> {
    type Output = Result<Fut::Output, PanicInfo>;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let this = self.project();
        let res = {
            let _guard = this.scheduler.set_current();
            std::panic::catch_unwind(AssertUnwindSafe(|| this.inner.poll(cx)))
        };
        match res {
            Ok(Poll::Pending) => Poll::Pending,
            Ok(Poll::Ready(res)) => Poll::Ready(Ok(res)),
            Err(panic) => {
                let msg = if let Some(s) = panic.downcast_ref::<String>() {
                    s.clone()
                } else {
                    "unknown panic".to_string()
                };
                let info = PanicInfo {
                    message: msg,
                    location: "".to_string(),
                    backtrace: "".to_string(),
                };
                Poll::Ready(Err(info))
            }
        }
    }
}

#[pin_project]
pub(crate) struct TaskFuture<Fut> {
    #[pin]
    inner: Fut,
    task_id: Option<TaskId>,
}

impl<Fut> TaskFuture<Fut> {
    pub(crate) fn new(inner: Fut, task_id: Option<TaskId>) -> Self {
        Self { inner, task_id }
    }
}

impl<Fut: Future> Future for TaskFuture<Fut> {
    type Output = Fut::Output;

    fn poll(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        let _guard = this
            .task_id
            .map(|task_id| CurrentTaskIdGuard::install(task_id));
        this.inner.poll(cx)
    }
}

pub(crate) fn current_task() -> Option<TaskId> {
    CURRENT_TASK.with_borrow(|t| t.clone())
}

struct CurrentTaskIdGuard {
    old_value: Option<TaskId>,
}

impl CurrentTaskIdGuard {
    fn install(task_id: TaskId) -> Self {
        let old_value = CURRENT_TASK.replace(Some(task_id));
        Self { old_value }
    }
}

impl Drop for CurrentTaskIdGuard {
    fn drop(&mut self) {
        CURRENT_TASK.with_borrow_mut(|v| *v = self.old_value);
    }
}
