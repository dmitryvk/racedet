use std::{
    collections::{HashMap, HashSet},
    sync::atomic::AtomicU64,
};

use crate::{
    TaskId,
    sync_model::{
        BadSyncError, DynSyncModel, NotificationOutcome, ProcessSyncEvent, SyncEvent, SyncModel,
        TaskProgressDependencies,
    },
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TaskGroup(u64);

impl TaskGroup {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        static NEXT_ID: AtomicU64 = AtomicU64::new(0);
        let id = NEXT_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Self(id)
    }
}

#[derive(Default)]
pub struct TaskWaitModel {
    task_groups: HashMap<TaskGroup, TaskGroupState>,
    task_waits: HashMap<TaskId, TaskWaitCondition>,
}

struct TaskGroupState {
    completed: HashSet<TaskGroupSlotIdx>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct TaskGroupSlotIdx(usize);

enum TaskWaitCondition {
    AnyN {
        task_group: TaskGroup,
        num_tasks: usize,
    },
    Specific {
        task_group: TaskGroup,
        slot: TaskGroupSlotIdx,
    },
}

impl SyncModel for TaskWaitModel {}

impl DynSyncModel for TaskWaitModel {
    fn task_progress_dependencies(&self, task_id: TaskId) -> TaskProgressDependencies {
        match self.task_waits.get(&task_id) {
            Some(TaskWaitCondition::AnyN {
                task_group,
                num_tasks,
            }) => {
                let Some(task_group) = self.task_groups.get(task_group) else {
                    return TaskProgressDependencies::Blocked;
                };
                if task_group.completed.len() >= *num_tasks {
                    TaskProgressDependencies::Ready {
                        need_to_run: HashSet::new(),
                    }
                } else {
                    TaskProgressDependencies::Blocked
                }
            }
            Some(TaskWaitCondition::Specific { task_group, slot }) => {
                let Some(task_group) = self.task_groups.get(task_group) else {
                    return TaskProgressDependencies::Blocked;
                };
                if task_group.completed.contains(slot) {
                    TaskProgressDependencies::Ready {
                        need_to_run: HashSet::new(),
                    }
                } else {
                    TaskProgressDependencies::Blocked
                }
            }
            None => TaskProgressDependencies::Ready {
                need_to_run: HashSet::new(),
            },
        }
    }
}

pub struct NewTaskGroup(pub TaskGroup);
pub struct FreeTaskGroup(pub TaskGroup);
pub struct TaskCompleted(pub TaskGroup, pub usize);
pub struct TaskWaitAnyN(pub TaskGroup, pub usize);
pub struct TaskWaitNth(pub TaskGroup, pub usize);
pub struct TaskWaitCompleted;

impl SyncEvent for NewTaskGroup {
    type Model = TaskWaitModel;
}
impl SyncEvent for FreeTaskGroup {
    type Model = TaskWaitModel;
}
impl SyncEvent for TaskCompleted {
    type Model = TaskWaitModel;
}
impl SyncEvent for TaskWaitAnyN {
    type Model = TaskWaitModel;
}
impl SyncEvent for TaskWaitNth {
    type Model = TaskWaitModel;
}
impl SyncEvent for TaskWaitCompleted {
    type Model = TaskWaitModel;
}

impl ProcessSyncEvent<NewTaskGroup> for TaskWaitModel {
    fn on_event(
        &mut self,
        _task_id: TaskId,
        NewTaskGroup(task_group): NewTaskGroup,
    ) -> Result<NotificationOutcome, BadSyncError> {
        self.task_groups.insert(
            task_group,
            TaskGroupState {
                completed: HashSet::new(),
            },
        );
        Ok(NotificationOutcome::Acknowledged)
    }
}

impl ProcessSyncEvent<FreeTaskGroup> for TaskWaitModel {
    fn on_event(
        &mut self,
        _task_id: TaskId,
        FreeTaskGroup(task_group): FreeTaskGroup,
    ) -> Result<NotificationOutcome, BadSyncError> {
        self.task_groups.remove(&task_group);
        Ok(NotificationOutcome::Acknowledged)
    }
}

impl ProcessSyncEvent<TaskCompleted> for TaskWaitModel {
    fn on_event(
        &mut self,
        _task_id: TaskId,
        TaskCompleted(task_group, slot_idx): TaskCompleted,
    ) -> Result<NotificationOutcome, BadSyncError> {
        if let Some(task_group) = self.task_groups.get_mut(&task_group) {
            task_group.completed.insert(TaskGroupSlotIdx(slot_idx));
        }
        Ok(NotificationOutcome::Acknowledged)
    }
}

impl ProcessSyncEvent<TaskWaitAnyN> for TaskWaitModel {
    fn on_event(
        &mut self,
        task_id: TaskId,
        TaskWaitAnyN(task_group, num_tasks): TaskWaitAnyN,
    ) -> Result<NotificationOutcome, BadSyncError> {
        self.task_waits.insert(
            task_id,
            TaskWaitCondition::AnyN {
                task_group,
                num_tasks,
            },
        );
        Ok(NotificationOutcome::ScheduleRequired)
    }
}

impl ProcessSyncEvent<TaskWaitNth> for TaskWaitModel {
    fn on_event(
        &mut self,
        task_id: TaskId,
        TaskWaitNth(task_group, slot_idx): TaskWaitNth,
    ) -> Result<NotificationOutcome, BadSyncError> {
        self.task_waits.insert(
            task_id,
            TaskWaitCondition::Specific {
                task_group,
                slot: TaskGroupSlotIdx(slot_idx),
            },
        );
        Ok(NotificationOutcome::ScheduleRequired)
    }
}

impl ProcessSyncEvent<TaskWaitCompleted> for TaskWaitModel {
    fn on_event(
        &mut self,
        task_id: TaskId,
        _: TaskWaitCompleted,
    ) -> Result<NotificationOutcome, BadSyncError> {
        self.task_waits.remove(&task_id);
        Ok(NotificationOutcome::Acknowledged)
    }
}
