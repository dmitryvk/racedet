use std::{
    cell::RefCell,
    num::NonZeroU64,
    sync::{Arc, Mutex},
};

use tokio::task::yield_now;

use crate::{TaskId, Trace};

thread_local! {
    static CURRENT_SCHEDULER: RefCell<Option<Arc<Scheduler>>> = RefCell::new(None);
}

pub(crate) struct Scheduler {
    inner: Mutex<Inner>,
}

struct Inner {
    next_task_id: u64,
    tasks: Vec<Task>,
    trace: Vec<(TaskId, String)>,
}

pub(crate) struct Task {
    id: TaskId,
    name: String,
}

impl Scheduler {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Scheduler {
            inner: Mutex::new(Inner {
                next_task_id: 1,
                tasks: Vec::new(),
                trace: Vec::new(),
            }),
        })
    }

    pub(crate) fn current() -> Option<Arc<Scheduler>> {
        CURRENT_SCHEDULER.with_borrow(|s| s.clone())
    }

    pub(crate) fn set_current(self: &Arc<Self>) -> CurrentSchedulerGuard {
        CURRENT_SCHEDULER.with_borrow_mut(|s| {
            let old_value = s.replace(self.clone());
            CurrentSchedulerGuard { old_value }
        })
    }

    pub(crate) fn register_task(&self, name: &str) -> TaskId {
        let mut inner = self.inner.lock().unwrap();
        let id = TaskId(NonZeroU64::new(inner.next_task_id).unwrap());
        inner.next_task_id += 1;
        inner.tasks.push(Task {
            id,
            name: name.to_string(),
        });
        id
    }

    pub(crate) fn on_task_started(&self, task_id: TaskId) {
        println!("task {task_id:?} started");
    }

    pub(crate) fn on_task_finished(&self, task_id: TaskId) {
        println!("task {task_id:?} finished");
    }

    pub(crate) async fn on_reached_point(&self, task_id: TaskId, name: &str) {
        println!("reached point {task_id:?} {name}");
        let mut inner = self.inner.lock().unwrap();
        inner.trace.push((task_id, name.to_string()));
        drop(inner);
        yield_now().await;
    }

    pub(crate) fn get_trace(&self) -> Trace {
        let inner = self.inner.lock().unwrap();
        Trace {
            trace: inner
                .trace
                .iter()
                .map(|(task_id, point)| {
                    let task = inner.tasks.get(task_id.0.get() as usize - 1).unwrap();
                    format!("task {} {}: {point}", task.id.0, task.name)
                })
                .collect(),
        }
    }

    pub(crate) async fn run_control_loop(&self) {
        println!("run control loop started");
        // todo
    }
}

pub(crate) struct CurrentSchedulerGuard {
    old_value: Option<Arc<Scheduler>>,
}

impl Drop for CurrentSchedulerGuard {
    fn drop(&mut self) {
        CURRENT_SCHEDULER.replace(self.old_value.take());
    }
}
