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
    TaskId, TaskStableId, Trace,
    full_trace::{FullTraceTaskId, TaskRef, TraceItem},
    scheduler::get_runnable_tasks::{
        NextSchedulerAction, TaskScheduleChoice, get_eligible_scheduler_choices,
    },
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
    next_stable_task_id: u64,
    task_selector: TaskSelector,
    tasks: Vec<Task>,
    trace: Vec<TraceEvent>,
    sync_model: SyncModelRegistry,
}

pub(crate) struct Task {
    id: TaskId,
    stable_id: Option<TaskStableId>,
    name: String,
    prev_suspend_point: Option<String>,
    state: TaskState,
}

#[derive(Debug)]
enum TaskState {
    // The task is ready to be executed (at suspension point)
    Suspended { point: String },
    // The task is currently running.
    // There can be more than one running task in case a task becomes blocked while it was running
    Running,
    // The task is finished
    Finished,
}

enum TraceEvent {
    TaskStarted(TaskId),
    TaskFinished(TaskId),
    TaskSuspended {
        task: TaskId,
        suspend_point: String,
    },
    AutoResumedTasks {
        resumed_tasks: Vec<TaskId>,
        running_tasks: Vec<TraceTaskSnapshot>,
        suspended_tasks: Vec<TraceTaskSnapshot>,
    },
    ScheduleDecision {
        resumed_tasks: Vec<TaskId>,
        running_tasks: Vec<TraceTaskSnapshot>,
        suspended_tasks: Vec<TraceTaskSnapshot>,
        options: Vec<Vec<TaskId>>,
    },
}

struct TraceTaskSnapshot {
    task_id: TaskId,
    point: Option<String>,
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
                next_stable_task_id: 1,
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
            stable_id: None,
            name,
            prev_suspend_point: None,
            state: TaskState::Running,
        });
        inner.trace.push(TraceEvent::TaskStarted(id));
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
        inner.trace.push(TraceEvent::TaskFinished(task_id));
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
                tracing::debug!("scheduler_notify.notify_waiters before");
                self.scheduler_notify.notify_waiters();
                tracing::debug!("scheduler_notify.notify_waiters done");
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
                self.scheduler_notify.notify_waiters();
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
                    inner.trace.push(TraceEvent::TaskSuspended {
                        task: task_id,
                        suspend_point: name.to_string(),
                    });
                }
                TaskState::Suspended { .. } | TaskState::Finished => {
                    panic!(
"task {} {} reached point {name}, but its state is not Running, but rather is {:?}.
  This might mean that an internal task concurrency is happening (e.g., join or FuturesUnordered).
  If this is the case, each spawned task must be wrapped with `task`",
                    task.id.0, task.name, task.state
                );
                }
            }
            task.state = TaskState::Suspended {
                point: name.to_string(),
            };
            drop(guard);
        }
        self.scheduler_notify.notify_waiters();
        loop {
            let mut notified = pin!(self.task_notify.notified());
            notified.as_mut().enable();
            {
                let mut inner = self.lock();
                let task = inner.tasks.get_mut(Self::task_idx(task_id)).unwrap();
                if matches!(task.state, TaskState::Running) {
                    tracing::debug!("task {task_id:?} resumed from {name}");
                    break;
                }
            }
            notified.await;
        }
    }

    pub(crate) fn get_trace(&self) -> Trace {
        let inner = self.lock();
        Trace {
            trace: inner
                .trace
                .iter()
                .map(|trace_event| match trace_event {
                    TraceEvent::TaskStarted(task_id) => {
                        TraceItem::TaskStarted(Self::make_task_ref(&inner, *task_id))
                    }
                    TraceEvent::TaskFinished(task_id) => {
                        TraceItem::TaskFinished(Self::make_task_ref(&inner, *task_id))
                    }
                    TraceEvent::TaskSuspended {
                        task,
                        suspend_point,
                    } => TraceItem::TaskSuspended {
                        task: Self::make_task_ref(&inner, *task),
                        suspend_point: suspend_point.clone(),
                    },
                    TraceEvent::AutoResumedTasks {
                        resumed_tasks,
                        running_tasks,
                        suspended_tasks,
                    } => TraceItem::AutoResumedTasks {
                        resumed_tasks: resumed_tasks
                            .iter()
                            .map(|task_id| Self::make_task_ref(&inner, *task_id))
                            .collect(),
                        running_tasks: running_tasks
                            .iter()
                            .map(|task_snapshot| Self::make_task_snapshot(&inner, task_snapshot))
                            .collect(),
                        suspended_tasks: suspended_tasks
                            .iter()
                            .map(|task_snapshot| Self::make_task_snapshot(&inner, task_snapshot))
                            .collect(),
                    },
                    TraceEvent::ScheduleDecision {
                        resumed_tasks,
                        running_tasks,
                        suspended_tasks,
                        options,
                    } => TraceItem::ScheduleDecision {
                        resumed_tasks: resumed_tasks
                            .iter()
                            .map(|task_id| Self::make_task_ref(&inner, *task_id))
                            .collect(),
                        running_tasks: running_tasks
                            .iter()
                            .map(|task_snapshot| Self::make_task_snapshot(&inner, task_snapshot))
                            .collect(),
                        suspended_tasks: suspended_tasks
                            .iter()
                            .map(|task_snapshot| Self::make_task_snapshot(&inner, task_snapshot))
                            .collect(),
                        options: options
                            .iter()
                            .map(|tasks| {
                                tasks
                                    .iter()
                                    .map(|task_id| Self::make_task_ref(&inner, *task_id))
                                    .collect()
                            })
                            .collect(),
                    },
                })
                .collect(),
        }
    }

    fn make_task_ref(inner: &Inner, task_id: TaskId) -> TaskRef {
        let task = &inner.tasks[Self::task_idx(task_id)];
        TaskRef {
            id: task
                .stable_id
                .map_or(FullTraceTaskId::Unstable(task.id), FullTraceTaskId::Stable),
            name: task.name.clone(),
        }
    }

    fn make_task_snapshot(
        inner: &Inner,
        task_snapshot: &TraceTaskSnapshot,
    ) -> crate::full_trace::TaskSnapshot {
        let task = &inner.tasks[Self::task_idx(task_snapshot.task_id)];
        crate::full_trace::TaskSnapshot {
            id: task
                .stable_id
                .map_or(FullTraceTaskId::Unstable(task.id), FullTraceTaskId::Stable),
            name: task.name.clone(),
            position: task_snapshot.point.clone(),
        }
    }

    pub(crate) async fn run_control_loop(&self) {
        tracing::debug!("run control loop started");
        loop {
            let mut notified = pin!(self.scheduler_notify.notified());
            notified.as_mut().enable();

            self.control_loop_iteration();

            notified.await;
        }
    }

    fn control_loop_iteration(&self) {
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
        {
            let mut new_tasks = inner
                        .tasks
                        .iter_mut()
                        .filter(|task| matches!(&task.state, TaskState::Suspended { .. } if task.stable_id.is_none()))
                        .peekable();
            if new_tasks.peek().is_some() {
                let mut new_tasks: Vec<&mut Task> = new_tasks.collect();
                new_tasks.sort_by(|task_1, task_2| {
                    (&task_1.name, task_1.id).cmp(&(&task_2.name, task_2.id))
                });
                for new_task in new_tasks {
                    let stable_id =
                        TaskStableId(NonZeroU64::new(inner.next_stable_task_id).unwrap());
                    inner.next_stable_task_id += 1;
                    new_task.stable_id = Some(stable_id);
                }
            }
        }

        let task_choices = {
            let mut running_tasks = HashSet::new();
            let mut suspended_tasks = HashSet::new();
            for task in &inner.tasks {
                match &task.state {
                    TaskState::Running => {
                        running_tasks.insert(task.id);
                    }
                    TaskState::Suspended { .. } => {
                        suspended_tasks.insert(task.id);
                    }
                    TaskState::Finished => {
                        // TODO: this state are unnecessary
                    }
                }
            }

            get_eligible_scheduler_choices(&running_tasks, &suspended_tasks, &inner.sync_model)
        };
        match task_choices {
            NextSchedulerAction::NoChoice(tasks) => {
                if tasks.is_empty() {
                    tracing::debug!("no ready tasks!");
                } else {
                    let (running_tasks, suspended_tasks) = Self::trace_task_snapshots(inner);
                    inner.trace.push(TraceEvent::AutoResumedTasks {
                        resumed_tasks: tasks.iter().copied().collect(),
                        running_tasks,
                        suspended_tasks,
                    });
                    self.resume_tasks(inner, &tasks);
                }
            }
            NextSchedulerAction::Choices(choices) if choices.is_empty() => {
                tracing::debug!("no ready tasks!");
            }
            NextSchedulerAction::Choices(choices) => {
                let task_choice = inner.task_selector.choose_next_running_task(&choices);

                tracing::debug!(
                    "chose {task_choice:?} out of {} options: {choices:?}",
                    choices.len()
                );
                let (running_tasks, suspended_tasks) = Self::trace_task_snapshots(inner);
                inner.trace.push(TraceEvent::ScheduleDecision {
                    resumed_tasks: task_choice.to_run.iter().copied().collect(),
                    running_tasks,
                    suspended_tasks,
                    options: choices
                        .iter()
                        .map(|choice| choice.to_run.iter().copied().collect())
                        .collect(),
                });

                self.resume_tasks(inner, &task_choice.to_run);
            }
        };

        drop(guard);
    }

    fn resume_tasks(&self, inner: &mut Inner, tasks: &HashSet<TaskId>) {
        if !tasks.is_empty() {
            for task_id in tasks {
                let task = inner.tasks.get_mut(Self::task_idx(*task_id)).unwrap();
                task.prev_suspend_point = match mem::replace(&mut task.state, TaskState::Running) {
                    TaskState::Suspended { point } => Some(point),
                    TaskState::Running | TaskState::Finished => {
                        panic!(
                            "Internal error: task {} {} is selected to run, but its state was not Suspended, but rather is {:?}.",
                            task.id.0, task.name, task.state
                        );
                    }
                };
                tracing::debug!("resuming task {:?}", task.id);
            }

            self.task_notify.notify_waiters();
        }
    }

    fn trace_task_snapshots(inner: &Inner) -> (Vec<TraceTaskSnapshot>, Vec<TraceTaskSnapshot>) {
        let mut running_tasks = Vec::new();
        let mut suspended_tasks = Vec::new();
        for task in &inner.tasks {
            let snapshot = TraceTaskSnapshot {
                task_id: task.id,
                point: match &task.state {
                    TaskState::Suspended { point } => Some(point.clone()),
                    TaskState::Running | TaskState::Finished => task.prev_suspend_point.clone(),
                },
            };
            match &task.state {
                TaskState::Suspended { .. } => suspended_tasks.push(snapshot),
                TaskState::Running => running_tasks.push(snapshot),
                TaskState::Finished => {}
            }
        }
        (running_tasks, suspended_tasks)
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
