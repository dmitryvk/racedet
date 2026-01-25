#[cfg(feature = "active")]
use std::collections::HashSet;

use crate::{
    sync::{
        BadSync, DynSyncModel, ProcessSyncEvent, SyncEvent, SyncModel, TaskProgressDependencies,
    },
    task::TaskId,
};

#[derive(Default, Debug)]
pub struct UnknownWaitModel {
    #[cfg(feature = "active")]
    blocked_tasks: HashSet<TaskId>,
}

impl SyncModel for UnknownWaitModel {}

impl DynSyncModel for UnknownWaitModel {
    fn task_progress_dependencies(&self, task_id: TaskId) -> TaskProgressDependencies {
        #[cfg(not(feature = "active"))]
        {
            _ = task_id;
            TaskProgressDependencies::ready()
        }
        #[cfg(feature = "active")]
        {
            if self.blocked_tasks.contains(&task_id) {
                TaskProgressDependencies::blocked_with_reason("unknown")
            } else {
                TaskProgressDependencies::ready()
            }
        }
    }
}

pub struct TaskWaitForUnknownStarted;
pub struct TaskWaitForUnknownCompleted;

impl SyncEvent for TaskWaitForUnknownStarted {
    type Model = UnknownWaitModel;
}
impl SyncEvent for TaskWaitForUnknownCompleted {
    type Model = UnknownWaitModel;
}

impl ProcessSyncEvent<TaskWaitForUnknownStarted> for UnknownWaitModel {
    fn on_event(&mut self, task_id: TaskId, _: TaskWaitForUnknownStarted) -> Result<(), BadSync> {
        #[cfg(not(feature = "active"))]
        {
            _ = task_id;
        }
        #[cfg(feature = "active")]
        {
            self.blocked_tasks.insert(task_id);
            tracing::debug!("task {task_id:?} is blocked on unknown");
        }
        Ok(())
    }
}

impl ProcessSyncEvent<TaskWaitForUnknownCompleted> for UnknownWaitModel {
    fn on_event(&mut self, task_id: TaskId, _: TaskWaitForUnknownCompleted) -> Result<(), BadSync> {
        #[cfg(not(feature = "active"))]
        {
            _ = task_id;
        }
        #[cfg(feature = "active")]
        {
            self.blocked_tasks.remove(&task_id);
            tracing::debug!("task {task_id:?} is no longer blocked on unknown");
        }
        Ok(())
    }
}
