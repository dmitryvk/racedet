use std::collections::{HashMap, HashSet};

use crate::{
    TaskId,
    sync_model::{
        BadSyncError, DynSyncModel, NotificationOutcome, ProcessSyncEvent, SyncEvent, SyncModel,
        TaskProgressDependencies,
    },
};

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct MutexId(String);

impl MutexId {
    pub fn new(id: String) -> Self {
        Self(id)
    }
}

#[derive(Default)]
pub struct MutexModel {
    held_by: HashMap<MutexId, TaskId>,
    waiting: HashMap<TaskId, HashSet<MutexId>>,
}

impl SyncModel for MutexModel {}
impl DynSyncModel for MutexModel {
    fn task_progress_dependencies(&self, task_id: TaskId) -> TaskProgressDependencies {
        if self.waiting.get(&task_id).is_some_and(|waiting| {
            waiting
                .iter()
                .any(|mutex_id| self.held_by.contains_key(mutex_id))
        }) {
            TaskProgressDependencies::Blocked
        } else {
            TaskProgressDependencies::Ready {
                need_to_run: HashSet::new(),
            }
        }
    }
}

pub struct LockingMutex(pub MutexId);
impl SyncEvent for LockingMutex {
    type Model = MutexModel;
}
pub struct AbortedLockingMutex(pub MutexId);
impl SyncEvent for AbortedLockingMutex {
    type Model = MutexModel;
}
pub struct LockedMutex(pub MutexId);
impl SyncEvent for LockedMutex {
    type Model = MutexModel;
}
pub struct ReleasedMutex(pub MutexId);
impl SyncEvent for ReleasedMutex {
    type Model = MutexModel;
}

impl ProcessSyncEvent<LockingMutex> for MutexModel {
    fn on_notified(
        &mut self,
        task_id: TaskId,
        LockingMutex(lock_id): LockingMutex,
    ) -> Result<NotificationOutcome, BadSyncError> {
        if self.held_by.get(&lock_id) == Some(&task_id) {
            return Err(BadSyncError("the task already holds the mutex".to_string()));
        }
        if self
            .waiting
            .get(&task_id)
            .is_some_and(|waiting| waiting.contains(&lock_id))
        {
            return Err(BadSyncError(
                "the task is already waiting for the mutex".to_string(),
            ));
        }
        self.waiting.entry(task_id).or_default().insert(lock_id);
        Ok(NotificationOutcome::Acknowledged)
    }
}

impl ProcessSyncEvent<AbortedLockingMutex> for MutexModel {
    fn on_notified(
        &mut self,
        task_id: TaskId,
        AbortedLockingMutex(lock_id): AbortedLockingMutex,
    ) -> Result<NotificationOutcome, BadSyncError> {
        let waiting = self
            .waiting
            .get_mut(&task_id)
            .ok_or_else(|| BadSyncError("the task is not waiting for the mutex".to_string()))?;
        if !waiting.remove(&lock_id) {
            return Err(BadSyncError(
                "the task is not waiting for the mutex".to_string(),
            ));
        }
        if waiting.is_empty() {
            self.waiting.remove(&task_id);
        }
        Ok(NotificationOutcome::Acknowledged)
    }
}

impl ProcessSyncEvent<LockedMutex> for MutexModel {
    fn on_notified(
        &mut self,
        task_id: TaskId,
        LockedMutex(lock_id): LockedMutex,
    ) -> Result<NotificationOutcome, BadSyncError> {
        let waiting = self
            .waiting
            .get_mut(&task_id)
            .ok_or_else(|| BadSyncError("the task is not waiting for the mutex".to_string()))?;
        if !waiting.remove(&lock_id) {
            return Err(BadSyncError(
                "the task is not waiting for the mutex".to_string(),
            ));
        }
        if waiting.is_empty() {
            self.waiting.remove(&task_id);
        }
        self.held_by.insert(lock_id, task_id);
        Ok(NotificationOutcome::Acknowledged)
    }
}

impl ProcessSyncEvent<ReleasedMutex> for MutexModel {
    fn on_notified(
        &mut self,
        task_id: TaskId,
        ReleasedMutex(lock_id): ReleasedMutex,
    ) -> Result<NotificationOutcome, BadSyncError> {
        if self.held_by.remove(&lock_id) != Some(task_id) {
            return Err(BadSyncError(
                "the task was not holding the lock".to_string(),
            ));
        }
        Ok(NotificationOutcome::Acknowledged)
    }
}
