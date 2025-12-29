use std::{
    cell::RefCell,
    collections::HashSet,
    mem,
    num::NonZeroU64,
    pin::pin,
    sync::{Arc, Mutex, MutexGuard},
};

use itertools::Itertools;

use crate::{
    TaskId, Trace,
    scheduler::get_runnable_tasks::{TaskScheduleChoice, get_eligible_scheduler_choices},
    sync_model::{BadSyncError, NotificationOutcome, SyncEvent, SyncInitEvent, SyncModelRegistry},
};

mod get_runnable_tasks;

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
    sync_model: SyncModelRegistry,
}

pub(crate) struct Task {
    id: TaskId,
    name: String,
    prev_suspend_point: String,
    state: TaskState,
}

#[derive(Debug)]
enum TaskState {
    // The task is ready to be executed (at suspension point)
    Ready { point: String },
    // The task is currently running.
    // There can be more than one running task in case a task becomes blocked while it was running
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
                sync_model: SyncModelRegistry::new(),
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

    pub(crate) fn register_task(&self, name: String) -> TaskId {
        let mut inner = self.lock();
        let id = TaskId(NonZeroU64::new(inner.next_task_id).unwrap());
        tracing::debug!("task {id:?} {name} registered");
        inner.next_task_id += 1;
        inner.tasks.push(Task {
            id,
            name,
            prev_suspend_point: "(start)".to_string(),
            state: TaskState::Running,
        });
        id
    }

    fn task_idx(task_id: TaskId) -> usize {
        task_id.0.get() as usize - 1
    }

    pub(crate) fn on_task_finished(&self, task_id: TaskId) {
        tracing::debug!("task {task_id:?} finished");
        let mut guard = self.lock();
        let inner = &mut *guard;
        let task = inner.tasks.get_mut(Self::task_idx(task_id)).unwrap();
        task.state = TaskState::Finished;
        inner
            .trace
            .push((task.id, format!("[{}->finish)", task.prev_suspend_point)));
        drop(guard);
        self.scheduler_notify.notify_waiters();
    }

    pub(crate) fn on_sync_event<T: SyncEvent>(
        &self,
        task_id: TaskId,
        event: T,
    ) -> Result<(), BadSyncError> {
        let mut inner = self.lock();
        match inner.sync_model.on_notified(task_id, event)? {
            NotificationOutcome::Acknowledged => {
                // do nothing
            }
            NotificationOutcome::ScheduleRequired => {
                // TODO: force reschedule
            }
        }

        Ok(())
    }

    pub(crate) fn on_sync_init_event<T: SyncInitEvent>(
        &self,
        event: T,
    ) -> Result<(), BadSyncError> {
        let mut inner = self.lock();
        match inner.sync_model.on_init_event(event)? {
            NotificationOutcome::Acknowledged => {
                // do nothing
            }
            NotificationOutcome::ScheduleRequired => {
                // TODO: force reschedule
            }
        }

        Ok(())
    }

    pub(crate) async fn on_reached_point(&self, task_id: TaskId, name: &str) {
        tracing::debug!("reached point {task_id:?} {name}");

        {
            let mut guard = self.lock();
            let inner = &mut *guard;
            let task = inner.tasks.get_mut(Self::task_idx(task_id)).unwrap();
            match &task.state {
                TaskState::Running => {
                    inner
                        .trace
                        .push((task.id, format!("[{}->{name})", task.prev_suspend_point)));
                }
                TaskState::Ready { .. } | TaskState::Finished => {
                    panic!(
"task {} {} reached point {name}, but its state is not Running, but rather is {:?}.
  This might mean that an internal task concurrency is happening (e.g., join or FuturesUnordered).
  If this is the case, each spawned task must be wrapped with `task`",
                    task.id.0, task.name, task.state
                );
                }
            }
            task.state = TaskState::Ready {
                point: name.to_string(),
            };
            drop(guard);
        }
        self.scheduler_notify.notify_waiters();
        loop {
            self.task_notify.notified().await;
            let mut inner = self.lock();
            let task = inner.tasks.get_mut(Self::task_idx(task_id)).unwrap();
            if matches!(task.state, TaskState::Running) {
                tracing::debug!("task {task_id:?} resumed from {name}");
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
        tracing::debug!("run control loop started");
        loop {
            let mut notified = pin!(self.scheduler_notify.notified());
            notified.as_mut().enable();

            {
                let mut guard = self.lock();
                let inner = &mut *guard;
                tracing::debug!(
                    "run control loop; tasks=[{}]",
                    inner
                        .tasks
                        .iter()
                        .take(10)
                        .map(|t| format!(
                            "{{ id={:?} name={} prev={:?} state={:?} }}",
                            t.id, t.name, t.prev_suspend_point, t.state
                        ))
                        .join(", ")
                );
                let task_choices = {
                    let mut running_tasks = HashSet::new();
                    let mut suspended_tasks = HashSet::new();
                    for task in &inner.tasks {
                        match &task.state {
                            TaskState::Running => {
                                running_tasks.insert(task.id);
                            }
                            TaskState::Ready { .. } => {
                                suspended_tasks.insert(task.id);
                            }
                            TaskState::Finished => {
                                // TODO: this state are unnecessary
                            }
                        }
                    }

                    get_eligible_scheduler_choices(
                        &running_tasks,
                        &suspended_tasks,
                        &inner.sync_model,
                    )
                };
                if !task_choices.is_empty() {
                    let task_choice = inner.task_selector.choose_next_running_task(&task_choices);
                    tracing::debug!(
                        "chose {task_choice:?} out of {} options: {task_choices:?}",
                        task_choices.len()
                    );
                    if !task_choice.to_run.is_empty() {
                        for task_id in &task_choice.to_run {
                            let task = inner.tasks.iter_mut().find(|t| t.id == *task_id).unwrap();
                            task.prev_suspend_point = match mem::replace(
                                &mut task.state,
                                TaskState::Running,
                            ) {
                                TaskState::Ready { point } => point,
                                TaskState::Running | TaskState::Finished => {
                                    panic!(
                                        "task {} {} is selected to run, but its state was not Ready, but rather is {:?}. This is an internal error in conc-checker.",
                                        task.id.0, task.name, task.state
                                    );
                                }
                            };
                            tracing::debug!("resuming task {:?}", task.id);
                        }
                        self.task_notify.notify_waiters();
                    } else {
                        tracing::debug!("no ready tasks!");
                    }
                } else {
                    tracing::debug!("run loop control: no tasks to resume");
                }

                drop(guard);
            }

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
    fn choose_next_running_task<'a>(
        &mut self,
        task_choices: &'a [TaskScheduleChoice],
    ) -> &'a TaskScheduleChoice {
        match self {
            TaskSelector::Random(random_task_selector) => {
                random_task_selector.choose_next_running_task(task_choices)
            }
        }
    }
}

impl RandomTaskSelector {
    fn choose_next_running_task<'a>(
        &mut self,
        task_choices: &'a [TaskScheduleChoice],
    ) -> &'a TaskScheduleChoice {
        let idx = rand::random_range(0..task_choices.len());
        &task_choices[idx]
    }
}
