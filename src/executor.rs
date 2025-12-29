use std::{cell::RefCell, task::Poll};

use pin_project::pin_project;

use crate::TaskId;

thread_local! {
    static CURRENT_TASK: RefCell<Option<TaskId>> = const { RefCell::new(None) };
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
        let _guard = this.task_id.map(CurrentTaskIdGuard::install);
        this.inner.poll(cx)
    }
}

pub(crate) fn current_task() -> Option<TaskId> {
    CURRENT_TASK.with_borrow(|t| *t)
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
