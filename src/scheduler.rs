use std::{
    cell::RefCell,
    mem,
    num::NonZeroU64,
    pin::pin,
    sync::{Arc, Mutex, MutexGuard},
};

use itertools::Itertools;

use crate::{TaskId, Trace};

thread_local! {
    static CURRENT_SCHEDULER: RefCell<Option<Arc<Scheduler>>> = const { RefCell::new(None) };
}

pub(crate) struct Scheduler {
    inner: Mutex<Inner>,
    scheduler_notify: tokio::sync::Notify,
    task_notify: tokio::sync::Notify,
}

struct Inner {
    next_task_id: u64,
    task_selector: TaskSelector,
    tasks: Vec<Task>,
    trace: Vec<(TaskId, String)>,
}

pub(crate) struct Task {
    id: TaskId,
    name: String,
    prev_suspend_point: Option<String>,
    state: TaskState,
}

#[derive(Debug)]
enum TaskState {
    // The task is registered, but not started.
    Pending,
    // The task is almost started, but waiting for other pending tasks.
    // All tasks progress to [`ReadyAtStart`] when there are no more pending tasks
    WaitingForOtherPending,
    // The task is ready to be executed (at the very beginning of the task code)
    ReadyAtStart,
    // The task is ready to be executed (at suspension point)
    ReadyAtPoint { point: String },
    // The task is ready to be executed (at suspension point)
    Unschedulable { interval_name: String },
    // The task is currently running
    Running,
    // The task is finished
    Finished,
}

pub(crate) enum TaskSelector {
    Random(RandomTaskSelector),
}

pub(crate) struct RandomTaskSelector;

impl RandomTaskSelector {
    pub(crate) fn new() -> Self {
        Self
    }
}

impl Scheduler {
    pub(crate) fn new(task_selector: TaskSelector) -> Arc<Self> {
        Arc::new(Scheduler {
            inner: Mutex::new(Inner {
                next_task_id: 1,
                task_selector,
                tasks: Vec::new(),
                trace: Vec::new(),
            }),
            scheduler_notify: tokio::sync::Notify::new(),
            task_notify: tokio::sync::Notify::new(),
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

    fn lock(&self) -> MutexGuard<'_, Inner> {
        match self.inner.lock() {
            Ok(guard) => guard,
            Err(err) => {
                self.inner.clear_poison();
                err.into_inner()
            }
        }
    }

    pub(crate) fn register_task(&self, name: &str) -> TaskId {
        let mut inner = self.lock();
        let id = TaskId(NonZeroU64::new(inner.next_task_id).unwrap());
        println!("task {id:?} {name} registered");
        inner.next_task_id += 1;
        inner.tasks.push(Task {
            id,
            name: name.to_string(),
            prev_suspend_point: None,
            state: TaskState::Pending,
        });
        id
    }

    fn task_idx(task_id: TaskId) -> usize {
        task_id.0.get() as usize - 1
    }

    pub(crate) async fn on_task_started(&self, task_id: TaskId) {
        println!("task {task_id:?} started");
        let mut inner = self.lock();
        let task = inner.tasks.get_mut(Self::task_idx(task_id)).unwrap();
        task.state = TaskState::WaitingForOtherPending;
        drop(inner);
        self.scheduler_notify.notify_waiters();
        loop {
            self.task_notify.notified().await;
            let mut inner = self.lock();
            let task = inner.tasks.get_mut(Self::task_idx(task_id)).unwrap();
            if matches!(task.state, TaskState::Running) {
                println!("task {task_id:?} resumed from start");
                break;
            }
        }
    }

    pub(crate) fn on_task_finished(&self, task_id: TaskId) {
        println!("task {task_id:?} finished");
        let mut guard = self.lock();
        let inner = &mut *guard;
        let task = inner.tasks.get_mut(Self::task_idx(task_id)).unwrap();
        task.state = TaskState::Finished;
        inner.trace.push((task.id, "finished".to_string()));
        drop(guard);
        self.scheduler_notify.notify_waiters();
    }

    pub(crate) async fn on_reached_point(&self, task_id: TaskId, name: &str) {
        println!("reached point {task_id:?} {name}");
        let mut inner = self.lock();
        let task = inner.tasks.get_mut(Self::task_idx(task_id)).unwrap();
        match &task.state {
            TaskState::Running => {}
            TaskState::Pending
            | TaskState::WaitingForOtherPending
            | TaskState::ReadyAtStart
            | TaskState::ReadyAtPoint { .. }
            | TaskState::Unschedulable { .. }
            | TaskState::Finished => {
                panic!(
"task {} {} reached point {name}, but its state is not Running, but rather is {:?}.
  This might mean that an internal task concurrency is happening (e.g., join or FuturesUnordered).
  If this is the case, each spawned task must be wrapped with `task`",
                    task.id.0, task.name, task.state
                );
            }
        }
        task.state = TaskState::ReadyAtPoint {
            point: name.to_string(),
        };
        drop(inner);
        self.scheduler_notify.notify_waiters();
        loop {
            self.task_notify.notified().await;
            let mut inner = self.lock();
            let task = inner.tasks.get_mut(Self::task_idx(task_id)).unwrap();
            if matches!(task.state, TaskState::Running) {
                println!("task {task_id:?} resumed from {name}");
                break;
            }
        }
    }

    pub(crate) fn on_task_unschedulable(&self, task_id: TaskId, name: &str) {
        println!("task {task_id:?} reached unschedulable interval {name}");
        let mut inner = self.lock();
        let task = inner.tasks.get_mut(Self::task_idx(task_id)).unwrap();
        match &task.state {
            TaskState::Running => {}
            TaskState::Pending
            | TaskState::WaitingForOtherPending
            | TaskState::ReadyAtStart
            | TaskState::ReadyAtPoint { .. }
            | TaskState::Finished
            | TaskState::Unschedulable { .. } => {
                panic!(
                    "task {} {} reached unschedulable interval {name}, but its state is not Running, but rather is {:?}.",
                    task.id.0, task.name, task.state
                );
            }
        }
        task.state = TaskState::Unschedulable {
            interval_name: name.to_string(),
        };
        drop(inner);
        self.scheduler_notify.notify_waiters();
    }

    pub(crate) async fn on_task_schedulable(&self, task_id: TaskId) {
        println!("task {task_id:?} leaves unschedulable interval");
        let mut inner = self.lock();
        let task = inner.tasks.get_mut(Self::task_idx(task_id)).unwrap();
        let interval_name = match &task.state {
            TaskState::Unschedulable { interval_name } => interval_name.clone(),
            TaskState::Running
            | TaskState::Pending
            | TaskState::WaitingForOtherPending
            | TaskState::ReadyAtStart
            | TaskState::ReadyAtPoint { .. }
            | TaskState::Finished => {
                panic!(
                    "task {} {} leabes unschedulable interval, but its state is not Unschedulable, but rather is {:?}.",
                    task.id.0, task.name, task.state
                );
            }
        };
        task.state = TaskState::ReadyAtPoint {
            point: interval_name.clone(),
        };
        drop(inner);
        self.scheduler_notify.notify_waiters();
        loop {
            self.task_notify.notified().await;
            let mut inner = self.lock();
            let task = inner.tasks.get_mut(Self::task_idx(task_id)).unwrap();
            if matches!(task.state, TaskState::Running) {
                println!("task {task_id:?} resumed from {interval_name}");
                break;
            }
        }
    }

    pub(crate) fn get_trace(&self) -> Trace {
        let inner = self.lock();
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
        loop {
            let mut notified = pin!(self.scheduler_notify.notified());
            notified.as_mut().enable();

            let mut guard = self.lock();
            let inner = &mut *guard;
            println!(
                "run control loop; tasks=[{}]",
                inner
                    .tasks
                    .iter()
                    .map(|t| format!(
                        "{{ id={:?} name={} prev={:?} state={:?} }}",
                        t.id, t.name, t.prev_suspend_point, t.state
                    ))
                    .join(", ")
            );
            let has_pending = inner
                .tasks
                .iter()
                .any(|task| matches!(task.state, TaskState::Pending));
            if !has_pending {
                for task in &mut inner.tasks {
                    if matches!(task.state, TaskState::WaitingForOtherPending) {
                        println!("task {:?} moved to ready", task.id);
                        task.state = TaskState::ReadyAtStart;
                    }
                }
            }
            let has_running = inner
                .tasks
                .iter()
                .any(|task| matches!(task.state, TaskState::Running));
            if !has_running {
                if let Some(next_running) =
                    inner.task_selector.choose_next_running_task(&inner.tasks)
                {
                    let task = inner.tasks.get_mut(next_running).unwrap();
                    task.prev_suspend_point =
                        match mem::replace(&mut task.state, TaskState::Running) {
                            TaskState::ReadyAtPoint { point } => {
                                inner.trace.push((task.id, point.clone()));
                                Some(point)
                            }
                            TaskState::ReadyAtStart => {
                                inner.trace.push((task.id, "start".to_string()));
                                None
                            }
                            _ => None,
                        };
                    println!("task {:?} is ready to run", task.id);
                    self.task_notify.notify_waiters();
                } else {
                    println!("no ready tasks!");
                }
            } else {
                println!("a task is already running");
            }
            drop(guard);

            notified.await;
        }
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

impl TaskSelector {
    fn choose_next_running_task(&mut self, tasks: &[Task]) -> Option<usize> {
        match self {
            TaskSelector::Random(random_task_selector) => {
                random_task_selector.choose_next_running_task(tasks)
            }
        }
    }
}

impl RandomTaskSelector {
    fn choose_next_running_task(&mut self, tasks: &[Task]) -> Option<usize> {
        let num_ready = tasks
            .iter()
            .filter(|task| {
                matches!(
                    task.state,
                    TaskState::ReadyAtPoint { .. } | TaskState::ReadyAtStart
                )
            })
            .count();
        if num_ready == 0 {
            return None;
        }
        let ord = rand::random_range(0..num_ready);
        let idx = tasks
            .iter()
            .enumerate()
            .filter(|(_, task)| {
                matches!(
                    task.state,
                    TaskState::ReadyAtPoint { .. } | TaskState::ReadyAtStart
                )
            })
            .nth(ord)
            .unwrap()
            .0;
        Some(idx)
    }
}
