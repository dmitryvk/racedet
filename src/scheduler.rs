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
    TaskId, TaskStableId, TraceView,
    full_trace::{FullTraceTaskId, TaskRef, TraceViewItem},
    replay_trace::ReplayTrace,
    scheduler::get_runnable_tasks::{
        NextSchedulerAction, TaskScheduleChoice, get_eligible_scheduler_choices,
    },
    string_pool::{StringIdx, StringPool},
    sync_model::{BadSync, NotificationOutcome, SyncEvent, SyncInitEvent, SyncModelRegistry},
};

mod get_runnable_tasks;

thread_local! {
    static CURRENT_SCHEDULER: RefCell<Option<Arc<Scheduler>>> = const { RefCell::new(None) };
}

pub(crate) struct Scheduler {
    string_pool: Arc<StringPool>,
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
    replay: Option<ReplayState>,
    error: Option<String>,
}

struct ReplayState {
    trace: ReplayTrace,
    next_step: usize,
}

pub(crate) struct Task {
    id: TaskId,
    stable_id: Option<TaskStableId>,
    name: StringIdx,
    prev_suspend_point: Option<StringIdx>,
    state: TaskState,
}

#[derive(Debug)]
enum TaskState {
    // The task is ready to be executed (at suspension point)
    Suspended { point: StringIdx },
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
        suspend_point: StringIdx,
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
    point: Option<StringIdx>,
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
    pub(crate) fn new(
        replay: Option<&crate::replay_trace_parsed::ReplayTrace>,
        task_selector: TaskSelector,
    ) -> Arc<Self> {
        let string_pool = Arc::new(StringPool::new());
        let replay = replay.map(|replay| {
            tracing::debug!("replaying {replay}");
            ReplayTrace::from_parsed(string_pool.clone(), replay)
        });
        Arc::new(Scheduler {
            string_pool,
            inner: Mutex::new(Inner {
                next_task_id: 1,
                next_stable_task_id: 1,
                task_selector,
                tasks: Vec::new(),
                trace: Vec::new(),
                sync_model: SyncModelRegistry::new(),
                replay: replay.map(|trace| ReplayState {
                    trace,
                    next_step: 0,
                }),
                error: None,
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

    pub(crate) fn string_pool(&self) -> Arc<StringPool> {
        self.string_pool.clone()
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
        let name = self.string_pool.intern(name);
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
        if inner.error.is_none() {
            inner.trace.push(TraceEvent::TaskStarted(id));
        }
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
        if inner.error.is_none() {
            inner.trace.push(TraceEvent::TaskFinished(task_id));
        }
        drop(guard);
        tracing::debug!(
            "scheduler_notify.notify_waiters before (due to task finished {task_id:?})"
        );
        self.scheduler_notify.notify_waiters();
    }

    pub(crate) fn on_sync_event<T: SyncEvent>(
        &self,
        task_id: TaskId,
        event: T,
    ) -> Result<(), BadSync> {
        let mut inner = self.lock();
        match inner.sync_model.on_notified(task_id, event)? {
            NotificationOutcome::Acknowledged => {
                // do nothing
            }
            NotificationOutcome::ScheduleRequired => {
                tracing::debug!(
                    "scheduler_notify.notify_waiters before (due to sync event {task_id:?} \
                     schedule required)"
                );
                self.scheduler_notify.notify_waiters();
            }
        }

        Ok(())
    }

    pub(crate) fn on_sync_init_event<T: SyncInitEvent>(&self, event: T) -> Result<(), BadSync> {
        let mut inner = self.lock();
        match inner.sync_model.on_init_event(event)? {
            NotificationOutcome::Acknowledged => {
                // do nothing
            }
            NotificationOutcome::ScheduleRequired => {
                tracing::debug!(
                    "scheduler_notify.notify_waiters before (due to sync init event schedule \
                     required)"
                );
                self.scheduler_notify.notify_waiters();
            }
        }

        Ok(())
    }

    pub(crate) async fn on_reached_point(&self, task_id: TaskId, name: &str) {
        tracing::debug!("reached point {task_id:?} {name}");
        let name = self.string_pool.intern(name);

        {
            let mut guard = self.lock();
            let inner = &mut *guard;
            let task = inner.tasks.get_mut(Self::task_idx(task_id)).unwrap();
            match &task.state {
                TaskState::Running => {
                    if inner.error.is_none() {
                        inner.trace.push(TraceEvent::TaskSuspended {
                            task: task_id,
                            suspend_point: name,
                        });
                    }
                }
                TaskState::Suspended { .. } | TaskState::Finished => {
                    panic!(
                        "task {} {} reached point {name}, but its state is not Running, but \
                         rather is {:?}.
  This might mean that an internal task concurrency is happening (e.g., join or FuturesUnordered).
  If this is the case, each spawned task must be wrapped with `task`",
                        task.id.0, task.name, task.state
                    );
                }
            }
            task.state = TaskState::Suspended { point: name };
            drop(guard);
        }
        tracing::debug!(
            "scheduler_notify.notify_waiters before (due to task {task_id:?} reached point {name})"
        );
        self.scheduler_notify.notify_waiters();
        loop {
            let mut notified = pin!(self.task_notify.notified());
            notified.as_mut().enable();
            {
                let mut inner = self.lock();
                if let Some(err) = &inner.error {
                    panic!("scheduler error: {err}");
                }
                let task = inner.tasks.get_mut(Self::task_idx(task_id)).unwrap();
                if matches!(task.state, TaskState::Running) {
                    tracing::debug!("task {task_id:?} resumed from {name}");
                    break;
                }
            }
            notified.await;
        }
    }

    pub(crate) async fn on_reached_point_with_event<T: SyncEvent>(
        &self,
        task_id: TaskId,
        name: &str,
        event: T,
    ) -> Result<(), BadSync> {
        tracing::debug!("reached point {task_id:?} {name}");
        let name = self.string_pool.intern(name);

        {
            let mut guard = self.lock();
            let inner = &mut *guard;
            match inner.sync_model.on_notified(task_id, event)? {
                NotificationOutcome::Acknowledged => {
                    // do nothing
                }
                NotificationOutcome::ScheduleRequired => {
                    tracing::debug!(
                        "scheduler_notify.notify_waiters before (due to task {task_id:?} reaching \
                         event ScheduleRequired before point {name})"
                    );
                    self.scheduler_notify.notify_waiters();
                }
            }

            let task = inner.tasks.get_mut(Self::task_idx(task_id)).unwrap();
            match &task.state {
                TaskState::Running => {
                    if inner.error.is_none() {
                        inner.trace.push(TraceEvent::TaskSuspended {
                            task: task_id,
                            suspend_point: name,
                        });
                    }
                }
                TaskState::Suspended { .. } | TaskState::Finished => {
                    panic!(
                        "task {} {} reached point {name}, but its state is not Running, but \
                         rather is {:?}.
  This might mean that an internal task concurrency is happening (e.g., join or FuturesUnordered).
  If this is the case, each spawned task must be wrapped with `task`",
                        task.id.0, task.name, task.state
                    );
                }
            }
            task.state = TaskState::Suspended { point: name };
            drop(guard);
        }
        tracing::debug!(
            "scheduler_notify.notify_waiters before (due to task {task_id:?} reaching point \
             {name} with event ScheduleRequired)"
        );
        self.scheduler_notify.notify_waiters();
        loop {
            let mut notified = pin!(self.task_notify.notified());
            notified.as_mut().enable();
            {
                let mut inner = self.lock();
                if let Some(err) = &inner.error {
                    panic!("scheduler error: {err}");
                }
                let task = inner.tasks.get_mut(Self::task_idx(task_id)).unwrap();
                if matches!(task.state, TaskState::Running) {
                    tracing::debug!("task {task_id:?} resumed from {name}");
                    break;
                }
            }
            notified.await;
        }

        Ok(())
    }

    pub(crate) fn get_trace(&self) -> TraceView {
        let inner = self.lock();
        TraceView {
            trace: inner
                .trace
                .iter()
                .map(|trace_event| match trace_event {
                    TraceEvent::TaskStarted(task_id) => {
                        TraceViewItem::TaskStarted(self.make_task_ref(&inner, *task_id))
                    }
                    TraceEvent::TaskFinished(task_id) => {
                        TraceViewItem::TaskFinished(self.make_task_ref(&inner, *task_id))
                    }
                    TraceEvent::TaskSuspended {
                        task,
                        suspend_point,
                    } => TraceViewItem::TaskSuspended {
                        task: self.make_task_ref(&inner, *task),
                        suspend_point: self
                            .string_pool
                            .get(*suspend_point)
                            .expect("string_pool has all string from trace"),
                    },
                    TraceEvent::AutoResumedTasks {
                        resumed_tasks,
                        running_tasks,
                        suspended_tasks,
                    } => TraceViewItem::AutoResumedTasks {
                        resumed_tasks: resumed_tasks
                            .iter()
                            .map(|task_id| self.make_task_ref(&inner, *task_id))
                            .collect(),
                        running_tasks: running_tasks
                            .iter()
                            .map(|task_snapshot| self.make_task_snapshot(&inner, task_snapshot))
                            .collect(),
                        suspended_tasks: suspended_tasks
                            .iter()
                            .map(|task_snapshot| self.make_task_snapshot(&inner, task_snapshot))
                            .collect(),
                    },
                    TraceEvent::ScheduleDecision {
                        resumed_tasks,
                        running_tasks,
                        suspended_tasks,
                        options,
                    } => TraceViewItem::ScheduleDecision {
                        resumed_tasks: resumed_tasks
                            .iter()
                            .map(|task_id| self.make_task_ref(&inner, *task_id))
                            .collect(),
                        running_tasks: running_tasks
                            .iter()
                            .map(|task_snapshot| self.make_task_snapshot(&inner, task_snapshot))
                            .collect(),
                        suspended_tasks: suspended_tasks
                            .iter()
                            .map(|task_snapshot| self.make_task_snapshot(&inner, task_snapshot))
                            .collect(),
                        options: options
                            .iter()
                            .map(|tasks| {
                                tasks
                                    .iter()
                                    .map(|task_id| self.make_task_ref(&inner, *task_id))
                                    .collect()
                            })
                            .collect(),
                    },
                })
                .collect(),
        }
    }

    fn make_task_ref(&self, inner: &Inner, task_id: TaskId) -> TaskRef {
        let task = &inner.tasks[Self::task_idx(task_id)];
        let name = self
            .string_pool
            .get(task.name)
            .expect("string_pool has all strings from tasks");
        TaskRef {
            id: task
                .stable_id
                .map_or(FullTraceTaskId::Unstable(task.id), FullTraceTaskId::Stable),
            name,
        }
    }

    fn make_task_snapshot(
        &self,
        inner: &Inner,
        task_snapshot: &TraceTaskSnapshot,
    ) -> crate::full_trace::TaskSnapshot {
        let task = &inner.tasks[Self::task_idx(task_snapshot.task_id)];
        let name = self
            .string_pool
            .get(task.name)
            .expect("string pool has all strings");
        let point = task_snapshot.point.map(|point| {
            self.string_pool
                .get(point)
                .expect("string pool has all strings")
        });
        crate::full_trace::TaskSnapshot {
            id: task
                .stable_id
                .map_or(FullTraceTaskId::Unstable(task.id), FullTraceTaskId::Stable),
            name,
            position: point,
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
                    "{{ id={:?}/{:?} name={} prev={:?} state={:?} }}",
                    t.id, t.stable_id, t.name, t.prev_suspend_point, t.state
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
        tracing::debug!("scheduler choices: {task_choices:?}");
        match task_choices {
            NextSchedulerAction::NoChoice(tasks) => {
                if tasks.is_empty() {
                    tracing::debug!("no ready tasks!");
                } else {
                    let (running_tasks, suspended_tasks) = Self::trace_task_snapshots(inner);
                    if inner.error.is_none() {
                        inner.trace.push(TraceEvent::AutoResumedTasks {
                            resumed_tasks: tasks.iter().copied().collect(),
                            running_tasks,
                            suspended_tasks,
                        });
                    }
                    self.resume_tasks(inner, &tasks);
                }
            }
            NextSchedulerAction::Choices(choices) if choices.is_empty() => {
                tracing::debug!("no ready tasks!");
            }
            NextSchedulerAction::Choices(choices) => {
                {
                    // stable IDs should be assigned at the last possible moment, i.e. when we're making a scheduling decision
                    let mut new_tasks = inner
                        .tasks
                        .iter_mut()
                        .filter(|task| matches!(&task.state, TaskState::Suspended { .. } if task.stable_id.is_none()))
                        .peekable();
                    if new_tasks.peek().is_some() {
                        let mut new_tasks: Vec<&mut Task> = new_tasks.collect();
                        new_tasks.sort_by(|task_1, task_2| {
                            (self.string_pool.get(task_1.name), task_1.id)
                                .cmp(&(self.string_pool.get(task_2.name), task_2.id))
                        });
                        for new_task in new_tasks {
                            let stable_id =
                                TaskStableId(NonZeroU64::new(inner.next_stable_task_id).unwrap());
                            inner.next_stable_task_id += 1;
                            new_task.stable_id = Some(stable_id);
                        }
                    }
                }

                let task_choice = if let Some(replay) = &mut inner.replay {
                    let suspended_tasks = inner
                        .tasks
                        .iter()
                        .filter_map(|task| match &task.state {
                            TaskState::Suspended { point } => Some((
                                task.stable_id.expect("suspended tasks have stable id"),
                                task.name,
                                *point,
                            )),
                            _ => None,
                        })
                        .collect::<HashSet<_>>();
                    let resumed_tasks = match replay
                        .trace
                        .get_resumed_tasks(replay.next_step, &suspended_tasks)
                    {
                        Ok(tasks) => tasks,
                        Err(err) => {
                            tracing::error!(
                                "task execution has diverged at step {}: {err}",
                                replay.next_step
                            );
                            let msg = format!(
                                "task execution has diverged at step {}: {err}",
                                replay.next_step
                            );
                            inner.error = Some(msg.clone());
                            self.task_notify.notify_waiters();
                            panic!("{msg}");
                        }
                    };
                    let Some(task_choice) = choices.iter().find(|choice| {
                        choice.to_run.len() == resumed_tasks.len()
                            && choice
                                .to_run
                                .iter()
                                .map(|id| {
                                    inner.tasks[Self::task_idx(*id)]
                                        .stable_id
                                        .expect("suspended task has stable id")
                                })
                                .collect::<HashSet<_>>()
                                == HashSet::from_iter(resumed_tasks.iter().copied())
                    }) else {
                        tracing::error!(
                            "task execution has diverged at step {}: did not find resumed_tasks \
                             {resumed_tasks:?} among possible choices {choices:?}",
                            replay.next_step
                        );
                        let msg = format!(
                            "task execution has diverged at step {}: did not find resumed_tasks \
                             {resumed_tasks:?} among possible choices {choices:?}",
                            replay.next_step
                        );
                        inner.error = Some(msg.clone());
                        self.task_notify.notify_waiters();
                        panic!("{msg}");
                    };
                    tracing::debug!("replayed step {}", replay.next_step);
                    replay.next_step += 1;
                    task_choice
                } else {
                    inner.task_selector.choose_next_running_task(&choices)
                };

                tracing::debug!(
                    "chose {task_choice:?} out of {} options: {choices:?}",
                    choices.len()
                );
                let (running_tasks, suspended_tasks) = Self::trace_task_snapshots(inner);
                if inner.error.is_none() {
                    inner.trace.push(TraceEvent::ScheduleDecision {
                        resumed_tasks: task_choice.to_run.iter().copied().collect(),
                        running_tasks,
                        suspended_tasks,
                        options: choices
                            .iter()
                            .map(|choice| choice.to_run.iter().copied().collect())
                            .collect(),
                    });
                }

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
                            "Internal error: task {} {} is selected to run, but its state was not \
                             Suspended, but rather is {:?}.",
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
                    TaskState::Suspended { point } => Some(*point),
                    TaskState::Running | TaskState::Finished => task.prev_suspend_point,
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
