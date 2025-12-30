use std::collections::HashSet;

use crate::{
    TaskId,
    sync_model::{
        BadSyncError, DynSyncModel, NotificationOutcome, ProcessSyncEvent, SyncEvent, SyncModel,
        TaskProgressDependencies,
    },
};

#[derive(Default)]
pub struct TaskJoinModel {
    /// Tasks that are performing `join`. These tasks are blocked.
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
    ) -> Result<NotificationOutcome, BadSyncError> {
        if !self.is_in_join.insert(task_id) {
            return Err(BadSyncError("task is already in join".to_string()));
        }

        Ok(NotificationOutcome::Acknowledged)
    }
}

impl ProcessSyncEvent<CompletedJoin> for TaskJoinModel {
    fn on_event(
        &mut self,
        task_id: TaskId,
        _event: CompletedJoin,
    ) -> Result<NotificationOutcome, BadSyncError> {
        if !self.is_in_join.remove(&task_id) {
            return Err(BadSyncError("task is not in a join".to_string()));
        }

        Ok(NotificationOutcome::Acknowledged)
    }
}

impl SyncModel for TaskJoinModel {}
impl DynSyncModel for TaskJoinModel {
    fn task_progress_dependencies(&self, task_id: TaskId) -> TaskProgressDependencies {
        if self.is_in_join.contains(&task_id) {
            TaskProgressDependencies::Blocked
        } else {
            TaskProgressDependencies::Ready {
                need_to_run: HashSet::new(),
            }
        }
    }
}
