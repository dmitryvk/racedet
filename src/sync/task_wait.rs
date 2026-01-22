#[cfg(feature = "active")]
use std::collections::{HashMap, HashSet};
use std::sync::atomic::AtomicU64;

use crate::{
    sync::{
        BadSync, DynSyncModel, ProcessSyncEvent, ProcessSyncInitEvent, SyncEvent, SyncInitEvent,
        SyncModel, TaskProgressDependencies,
    },
    task::TaskId,
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

#[derive(Default, Debug)]
pub struct TaskWaitModel {
    #[cfg(feature = "active")]
    task_groups: HashMap<TaskGroup, TaskGroupState>,
    #[cfg(feature = "active")]
    task_waits: HashMap<TaskId, TaskWaitCondition>,
}

#[cfg(feature = "active")]
#[derive(Debug)]
struct TaskGroupState {
    completed: HashSet<TaskGroupSlotIdx>,
    spawned: HashSet<TaskGroupSlotIdx>,
    started: HashSet<TaskGroupSlotIdx>,
}

#[cfg(feature = "active")]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct TaskGroupSlotIdx(usize);

#[cfg(feature = "active")]
#[derive(Debug)]
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
        #[cfg(not(feature = "active"))]
        {
            _ = task_id;
            TaskProgressDependencies::ready()
        }
        #[cfg(feature = "active")]
        {
            match self.task_waits.get(&task_id) {
                Some(TaskWaitCondition::AnyN {
                    task_group,
                    num_tasks,
                }) => {
                    let Some(task_group) = self.task_groups.get(task_group) else {
                        return TaskProgressDependencies::Blocked;
                    };
                    if task_group
                        .spawned
                        .iter()
                        .any(|slot| !task_group.started.contains(slot))
                    {
                        // waiting for spawned tasks to be started
                        return TaskProgressDependencies::Ready {
                            need_to_run: HashSet::new(),
                        };
                    }
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
}

pub struct NewTaskGroup(pub TaskGroup);
pub struct FreeTaskGroup(pub TaskGroup);
pub struct TaskSpawned(pub TaskGroup, pub usize);
pub struct TaskStarted(pub TaskGroup, pub usize);
pub struct TaskCompleted(pub TaskGroup, pub usize);
pub struct TaskWaitAnyN(pub TaskGroup, pub usize);
pub struct TaskWaitNth(pub TaskGroup, pub usize);
pub struct TaskWaitCompleted;

impl SyncEvent for NewTaskGroup {
    type Model = TaskWaitModel;
}
impl SyncInitEvent for NewTaskGroup {
    type Model = TaskWaitModel;
}
impl SyncEvent for FreeTaskGroup {
    type Model = TaskWaitModel;
}
impl SyncEvent for TaskSpawned {
    type Model = TaskWaitModel;
}
impl SyncEvent for TaskStarted {
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
        task_id: TaskId,
        NewTaskGroup(task_group): NewTaskGroup,
    ) -> Result<(), BadSync> {
        #[cfg(not(feature = "active"))]
        {
            _ = task_id;
            _ = task_group;
        }
        #[cfg(feature = "active")]
        {
            self.task_groups.insert(
                task_group,
                TaskGroupState {
                    completed: HashSet::new(),
                    spawned: HashSet::new(),
                    started: HashSet::new(),
                },
            );
            tracing::debug!(
                "task {task_id:?} registered task group {task_group:?}, new state: {self:?}"
            );
        }
        Ok(())
    }
}

impl ProcessSyncInitEvent<NewTaskGroup> for TaskWaitModel {
    fn on_init_event(&mut self, NewTaskGroup(task_group): NewTaskGroup) -> Result<(), BadSync> {
        #[cfg(not(feature = "active"))]
        {
            _ = task_group;
        }
        #[cfg(feature = "active")]
        {
            self.task_groups.insert(
                task_group,
                TaskGroupState {
                    completed: HashSet::new(),
                    spawned: HashSet::new(),
                    started: HashSet::new(),
                },
            );
            tracing::debug!("registered task group {task_group:?}, new state: {self:?}");
        }
        Ok(())
    }
}

impl ProcessSyncEvent<FreeTaskGroup> for TaskWaitModel {
    fn on_event(
        &mut self,
        task_id: TaskId,
        FreeTaskGroup(task_group): FreeTaskGroup,
    ) -> Result<(), BadSync> {
        #[cfg(not(feature = "active"))]
        {
            _ = task_id;
            _ = task_group;
        }
        #[cfg(feature = "active")]
        {
            self.task_groups.remove(&task_group);
            tracing::debug!(
                "task {task_id:?} removed task group {task_group:?}, new state: {self:?}"
            );
        }
        Ok(())
    }
}

impl ProcessSyncEvent<TaskSpawned> for TaskWaitModel {
    fn on_event(
        &mut self,
        task_id: TaskId,
        TaskSpawned(task_group, slot_idx): TaskSpawned,
    ) -> Result<(), BadSync> {
        #[cfg(not(feature = "active"))]
        {
            _ = task_id;
            _ = task_group;
            _ = slot_idx;
        }
        #[cfg(feature = "active")]
        {
            if let Some(task_group) = self.task_groups.get_mut(&task_group) {
                task_group.spawned.insert(TaskGroupSlotIdx(slot_idx));
            }
            tracing::debug!(
                "task {task_id:?} spawned {task_group:?} {slot_idx}, new state: {self:?}"
            );
        }
        Ok(())
    }
}

impl ProcessSyncEvent<TaskStarted> for TaskWaitModel {
    fn on_event(
        &mut self,
        task_id: TaskId,
        TaskStarted(task_group, slot_idx): TaskStarted,
    ) -> Result<(), BadSync> {
        #[cfg(not(feature = "active"))]
        {
            _ = task_id;
            _ = task_group;
            _ = slot_idx;
        }
        #[cfg(feature = "active")]
        {
            if let Some(task_group) = self.task_groups.get_mut(&task_group) {
                task_group.started.insert(TaskGroupSlotIdx(slot_idx));
            }
            tracing::debug!(
                "task {task_id:?} started {task_group:?} {slot_idx}, new state: {self:?}"
            );
        }
        Ok(())
    }
}

impl ProcessSyncEvent<TaskCompleted> for TaskWaitModel {
    fn on_event(
        &mut self,
        task_id: TaskId,
        TaskCompleted(task_group, slot_idx): TaskCompleted,
    ) -> Result<(), BadSync> {
        #[cfg(not(feature = "active"))]
        {
            _ = task_id;
            _ = task_group;
            _ = slot_idx;
        }
        #[cfg(feature = "active")]
        {
            if let Some(task_group) = self.task_groups.get_mut(&task_group) {
                task_group.completed.insert(TaskGroupSlotIdx(slot_idx));
            }
            tracing::debug!(
                "task {task_id:?} completed {task_group:?} {slot_idx}, new state: {self:?}"
            );
        }
        Ok(())
    }
}

impl ProcessSyncEvent<TaskWaitAnyN> for TaskWaitModel {
    fn on_event(
        &mut self,
        task_id: TaskId,
        TaskWaitAnyN(task_group, num_tasks): TaskWaitAnyN,
    ) -> Result<(), BadSync> {
        #[cfg(not(feature = "active"))]
        {
            _ = task_id;
            _ = task_group;
            _ = num_tasks;
        }
        #[cfg(feature = "active")]
        {
            self.task_waits.insert(
                task_id,
                TaskWaitCondition::AnyN {
                    task_group,
                    num_tasks,
                },
            );
            tracing::debug!(
                "task wait AnyN({task_group:?}, {num_tasks}) registered for {task_id:?}, new \
                 state: {self:?}"
            );
        }
        Ok(())
    }
}

impl ProcessSyncEvent<TaskWaitNth> for TaskWaitModel {
    fn on_event(
        &mut self,
        task_id: TaskId,
        TaskWaitNth(task_group, slot_idx): TaskWaitNth,
    ) -> Result<(), BadSync> {
        #[cfg(not(feature = "active"))]
        {
            _ = task_id;
            _ = task_group;
            _ = slot_idx;
        }
        #[cfg(feature = "active")]
        {
            self.task_waits.insert(
                task_id,
                TaskWaitCondition::Specific {
                    task_group,
                    slot: TaskGroupSlotIdx(slot_idx),
                },
            );
            tracing::debug!(
                "task wait Nth({task_group:?}, {slot_idx}) registered for {task_id:?}, new state: \
                 {self:?}"
            );
        }
        Ok(())
    }
}

impl ProcessSyncEvent<TaskWaitCompleted> for TaskWaitModel {
    fn on_event(&mut self, task_id: TaskId, _: TaskWaitCompleted) -> Result<(), BadSync> {
        #[cfg(not(feature = "active"))]
        {
            _ = task_id;
        }
        #[cfg(feature = "active")]
        {
            self.task_waits.remove(&task_id);
            tracing::debug!("task wait completed for {task_id:?}, new state: {self:?}");
        }
        Ok(())
    }
}
