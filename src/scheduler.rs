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
    SyncOperation, TaskId, TaskStartBarrierId, Trace,
    lock_model::{BoxDynSyncOperation, ErasedSyncState},
    scheduler::get_runnable_tasks::{TaskScheduleChoice, get_eligible_scheduler_choices},
    sync_model::{BadSyncError, NotificationOutcome, SyncEvent, SyncModelRegistry},
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
    next_task_start_barrier_id: u64,
    task_selector: TaskSelector,
    tasks: Vec<Task>,
    task_start_barriers: Vec<TaskStartBarrier>,
    locks: ErasedSyncState,
    trace: Vec<(TaskId, String)>,
    sync_model: SyncModelRegistry,
}

pub(crate) struct Task {
    id: TaskId,
    name: String,
    prev_suspend_point: String,
    state: TaskState,
}

pub(crate) struct TaskStartBarrier {
    #[expect(dead_code, reason = "might be used")]
    id: TaskStartBarrierId,
    name: String,
    num_tasks: usize,
    num_tasks_started: usize,
}

#[derive(Debug)]
enum TaskState {
    // The task is registered, but not started.
    Pending {
        start_barrier: Option<TaskStartBarrierId>,
    },
    // The task is almost started, but waiting for other pending tasks with the same `barrier_id`.
    WaitingAtStartBarrier {
        barrier_id: TaskStartBarrierId,
    },
    // The task is ready to be executed (at the very beginning of the task code)
    ReadyAtStart,
    // The task is ready to be executed (at suspension point)
    ReadyAtPoint {
        point: String,
        sync_operation: Option<BoxDynSyncOperation>,
    },
    // The task is not ready to be executed (at suspension point)
    Unschedulable {
        interval_name: String,
    },
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
                next_task_start_barrier_id: 1,
                task_selector,
                tasks: Vec::new(),
                task_start_barriers: Vec::new(),
                locks: ErasedSyncState::new(),
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

    pub(crate) fn register_task_start_barrier(
        &self,
        name: &str,
        num_tasks: usize,
    ) -> TaskStartBarrierId {
        let mut inner = self.lock();
        let id = TaskStartBarrierId(NonZeroU64::new(inner.next_task_start_barrier_id).unwrap());
        tracing::debug!("task start barrier {id:?} {name} registered");
        inner.next_task_start_barrier_id += 1;
        inner.task_start_barriers.push(TaskStartBarrier {
            id,
            name: name.to_string(),
            num_tasks,
            num_tasks_started: 0,
        });
        id
    }

    pub(crate) fn register_task(
        &self,
        name: &str,
        start_barrier: Option<TaskStartBarrierId>,
    ) -> TaskId {
        let mut inner = self.lock();
        let id = TaskId(NonZeroU64::new(inner.next_task_id).unwrap());
        tracing::debug!("task {id:?} {name} registered");
        inner.next_task_id += 1;
        inner.tasks.push(Task {
            id,
            name: name.to_string(),
            prev_suspend_point: "(start)".to_string(),
            state: TaskState::Pending { start_barrier },
        });
        id
    }

    fn task_idx(task_id: TaskId) -> usize {
        task_id.0.get() as usize - 1
    }

    pub(crate) async fn on_task_started(&self, task_id: TaskId) {
        tracing::debug!("task {task_id:?} started");
        {
            let mut guard = self.lock();
            let inner = &mut *guard;
            let task = inner.tasks.get_mut(Self::task_idx(task_id)).unwrap();
            let start_barrier = match &task.state {
                TaskState::Pending { start_barrier } => *start_barrier,
                TaskState::WaitingAtStartBarrier { .. }
                | TaskState::ReadyAtStart
                | TaskState::ReadyAtPoint { .. }
                | TaskState::Unschedulable { .. }
                | TaskState::Running
                | TaskState::Finished => {
                    panic!(
                        "task {} {} is started, but its state is not Pending, but rather is {:?}.
  This might mean that an internal task concurrency is happening (e.g., join or FuturesUnordered).
  If this is the case, each spawned task must be wrapped with `task`",
                        task.id.0, task.name, task.state
                    );
                }
            };
            task.state = if let Some(start_barrier) = start_barrier {
                let barrier = inner
                    .task_start_barriers
                    .get_mut(usize::try_from(start_barrier.0.get() - 1).unwrap())
                    .unwrap();
                if barrier.num_tasks_started >= barrier.num_tasks {
                    panic!("too much tasks started in barrier {}", barrier.name);
                }
                barrier.num_tasks_started += 1;
                if barrier.num_tasks == barrier.num_tasks_started {
                    tracing::debug!("all tasks started in barrier {}", barrier.name);
                }
                TaskState::WaitingAtStartBarrier {
                    barrier_id: start_barrier,
                }
            } else {
                TaskState::ReadyAtStart
            };
        }
        self.scheduler_notify.notify_waiters();
        loop {
            self.task_notify.notified().await;
            let mut inner = self.lock();
            let task = inner.tasks.get_mut(Self::task_idx(task_id)).unwrap();
            if matches!(task.state, TaskState::Running) {
                tracing::debug!("task {task_id:?} resumed from start");
                break;
            }
        }
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

    pub(crate) async fn on_reached_point<S: SyncOperation>(
        &self,
        task_id: TaskId,
        name: &str,
        sync_operation: Option<S>,
    ) {
        if let Some(sync_op) = &sync_operation {
            tracing::debug!("reached point {task_id:?} {name} with {sync_op:?}");
        } else {
            tracing::debug!("reached point {task_id:?} {name}");
        }

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
                TaskState::Pending { .. }
                | TaskState::WaitingAtStartBarrier { .. }
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
            let sync_operation = sync_operation.map(|op| inner.locks.make_dyn_operation(op));
            task.state = TaskState::ReadyAtPoint {
                point: name.to_string(),
                sync_operation,
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

    pub(crate) fn on_task_unschedulable(&self, task_id: TaskId, name: &str) {
        tracing::debug!("task {task_id:?} reached unschedulable interval {name}");
        let mut guard = self.lock();
        let inner = &mut *guard;
        let task = inner.tasks.get_mut(Self::task_idx(task_id)).unwrap();
        match &task.state {
            TaskState::Running => {
                inner.trace.push((
                    task.id,
                    format!("[{}->{name}:start (and running)", task.prev_suspend_point),
                ));
            }
            TaskState::Pending { .. }
            | TaskState::WaitingAtStartBarrier { .. }
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
        task.prev_suspend_point = name.to_string();
        task.state = TaskState::Unschedulable {
            interval_name: name.to_string(),
        };
        drop(guard);
        self.scheduler_notify.notify_waiters();
    }

    pub(crate) async fn on_task_schedulable(&self, task_id: TaskId) {
        tracing::debug!("task {task_id:?} leaves unschedulable interval");
        let interval_name;
        {
            let mut guard = self.lock();
            let inner = &mut *guard;
            let task = inner.tasks.get_mut(Self::task_idx(task_id)).unwrap();
            interval_name = match &task.state {
                TaskState::Unschedulable { interval_name } => interval_name.clone(),
                TaskState::Running
                | TaskState::Pending { .. }
                | TaskState::WaitingAtStartBarrier { .. }
                | TaskState::ReadyAtStart
                | TaskState::ReadyAtPoint { .. }
                | TaskState::Finished => {
                    panic!(
                        "task {} {} leaves unschedulable interval, but its state is not Unschedulable, but rather is {:?}.",
                        task.id.0, task.name, task.state
                    );
                }
            };
            inner.trace.push((
                task.id,
                format!("[{interval_name}:start->{interval_name}:end)"),
            ));
            task.state = TaskState::ReadyAtPoint {
                point: interval_name.clone(),
                sync_operation: None,
            };
        }
        self.scheduler_notify.notify_waiters();
        loop {
            self.task_notify.notified().await;
            let mut inner = self.lock();
            let task = inner.tasks.get_mut(Self::task_idx(task_id)).unwrap();
            if matches!(task.state, TaskState::Running) {
                tracing::debug!("task {task_id:?} resumed from {interval_name}");
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

    pub(crate) async fn run_control_loop(&self, stop_barrier: Option<TaskStartBarrierId>) {
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
                for task in &mut inner.tasks {
                    if let TaskState::WaitingAtStartBarrier { barrier_id } = &task.state {
                        let barrier = inner
                            .task_start_barriers
                            .get(usize::try_from(barrier_id.0.get() - 1).unwrap())
                            .unwrap();
                        if barrier.num_tasks_started == barrier.num_tasks {
                            task.state = TaskState::ReadyAtStart;
                            tracing::debug!("task {:?} is moved from Barrier to Ready", task.id);
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
                            TaskState::ReadyAtStart | TaskState::ReadyAtPoint { .. } => {
                                suspended_tasks.insert(task.id);
                            }
                            TaskState::Pending { .. }
                            | TaskState::WaitingAtStartBarrier { .. }
                            | TaskState::Unschedulable { .. }
                            | TaskState::Finished => {
                                // TODO: these states are unnecessary
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
                                TaskState::ReadyAtPoint {
                                    point,
                                    sync_operation,
                                } => {
                                    if let Some(sync_operation) = sync_operation {
                                        inner
                                            .locks
                                            .task_selected_for_running(task.id, &sync_operation)
                                            .expect("usage should be correct");
                                    }
                                    point
                                }
                                TaskState::ReadyAtStart => "start".to_string(),
                                TaskState::Pending { .. }
                                | TaskState::WaitingAtStartBarrier { .. }
                                | TaskState::Unschedulable { .. }
                                | TaskState::Running
                                | TaskState::Finished => {
                                    panic!(
                                        "task {} {} is selected to run, but its state was not ReadyAtPoint/ReadyAtStart, but rather is {:?}. This is an internal error in conc-checker.",
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
                    tracing::debug!("run loop control: a task is already running");
                }

                if let Some(stop_barrier) = stop_barrier
                    && let Some(barrier) = inner
                        .task_start_barriers
                        .get(usize::try_from(stop_barrier.0.get() - 1).unwrap())
                    && barrier.num_tasks == barrier.num_tasks_started
                    && inner
                        .tasks
                        .iter()
                        .all(|task| matches!(task.state, TaskState::Finished))
                {
                    break;
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
