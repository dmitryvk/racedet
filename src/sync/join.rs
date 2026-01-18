use std::collections::HashSet;

use crate::{
    sync::{
        BadSync, DynSyncModel, NotificationOutcome, ProcessSyncEvent, SyncEvent, SyncModel,
        TaskProgressDependencies,
    },
    task::TaskId,
};

#[derive(Default)]
pub struct TaskJoinModel {
    /// Tasks that are performing `join`. These tasks are blocked.
    #[cfg(feature = "active")]
    is_in_join: HashSet<TaskId>,
}

pub struct StartingJoin;
impl SyncEvent for StartingJoin {
    type Model = TaskJoinModel;
}
pub struct CompletedJoin;
impl SyncEvent for CompletedJoin {
    type Model = TaskJoinModel;
}

impl ProcessSyncEvent<StartingJoin> for TaskJoinModel {
    fn on_event(
        &mut self,
        task_id: TaskId,
        _event: StartingJoin,
    ) -> Result<NotificationOutcome, BadSync> {
        #[cfg(not(feature = "active"))]
        {
            _ = task_id;
        }
        #[cfg(feature = "active")]
        if !self.is_in_join.insert(task_id) {
            return Err(BadSync("task is already in join".to_string()));
        }

        Ok(NotificationOutcome::Acknowledged)
    }
}

impl ProcessSyncEvent<CompletedJoin> for TaskJoinModel {
    fn on_event(
        &mut self,
        task_id: TaskId,
        _event: CompletedJoin,
    ) -> Result<NotificationOutcome, BadSync> {
        #[cfg(not(feature = "active"))]
        {
            _ = task_id;
        }
        #[cfg(feature = "active")]
        if !self.is_in_join.remove(&task_id) {
            return Err(BadSync("task is not in a join".to_string()));
        }

        Ok(NotificationOutcome::Acknowledged)
    }
}

impl SyncModel for TaskJoinModel {}
impl DynSyncModel for TaskJoinModel {
    fn task_progress_dependencies(&self, task_id: TaskId) -> TaskProgressDependencies {
        #[cfg(not(feature = "active"))]
        {
            _ = task_id;
            TaskProgressDependencies::Ready {
                need_to_run: HashSet::new(),
            }
        }
        #[cfg(feature = "active")]
        if self.is_in_join.contains(&task_id) {
            TaskProgressDependencies::Blocked
        } else {
            TaskProgressDependencies::Ready {
                need_to_run: HashSet::new(),
            }
        }
    }
}
