#[cfg(feature = "active")]
use std::collections::HashMap;
use std::collections::HashSet;

use crate::{
    sync::{
        BadSync, DynSyncModel, NotificationOutcome, ProcessSyncEvent, SyncEvent, SyncModel,
        TaskProgressDependencies,
    },
    task::TaskId,
};

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct MutexId(#[cfg(feature = "active")] String);

impl MutexId {
    pub fn new(id: impl Into<String>) -> Self {
        #[cfg(not(feature = "active"))]
        {
            _ = id;
            Self()
        }
        #[cfg(feature = "active")]
        Self(id.into())
    }
}

#[derive(Default)]
pub struct MutexModel {
    #[cfg(feature = "active")]
    held_by: HashMap<MutexId, TaskId>,
    #[cfg(feature = "active")]
    waiting: HashMap<TaskId, HashSet<MutexId>>,
}

impl SyncModel for MutexModel {}
impl DynSyncModel for MutexModel {
    fn task_progress_dependencies(&self, task_id: TaskId) -> TaskProgressDependencies {
        #[cfg(not(feature = "active"))]
        {
            _ = task_id;
            TaskProgressDependencies::Ready {
                need_to_run: HashSet::new(),
            }
        }
        #[cfg(feature = "active")]
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
    fn on_event(
        &mut self,
        task_id: TaskId,
        LockingMutex(lock_id): LockingMutex,
    ) -> Result<NotificationOutcome, BadSync> {
        #[cfg(not(feature = "active"))]
        {
            _ = task_id;
            _ = lock_id;
        }
        #[cfg(feature = "active")]
        {
            if self.held_by.get(&lock_id) == Some(&task_id) {
                return Err(BadSync("the task already holds the mutex".to_string()));
            }
            if self
                .waiting
                .get(&task_id)
                .is_some_and(|waiting| waiting.contains(&lock_id))
            {
                return Err(BadSync(
                    "the task is already waiting for the mutex".to_string(),
                ));
            }
            self.waiting.entry(task_id).or_default().insert(lock_id);
        }
        Ok(NotificationOutcome::Acknowledged)
    }
}

impl ProcessSyncEvent<AbortedLockingMutex> for MutexModel {
    fn on_event(
        &mut self,
        task_id: TaskId,
        AbortedLockingMutex(lock_id): AbortedLockingMutex,
    ) -> Result<NotificationOutcome, BadSync> {
        #[cfg(not(feature = "active"))]
        {
            _ = task_id;
            _ = lock_id;
        }
        #[cfg(feature = "active")]
        {
            let waiting = self
                .waiting
                .get_mut(&task_id)
                .ok_or_else(|| BadSync("the task is not waiting for the mutex".to_string()))?;
            if !waiting.remove(&lock_id) {
                return Err(BadSync("the task is not waiting for the mutex".to_string()));
            }
            if waiting.is_empty() {
                self.waiting.remove(&task_id);
            }
        }
        Ok(NotificationOutcome::Acknowledged)
    }
}

impl ProcessSyncEvent<LockedMutex> for MutexModel {
    fn on_event(
        &mut self,
        task_id: TaskId,
        LockedMutex(lock_id): LockedMutex,
    ) -> Result<NotificationOutcome, BadSync> {
        #[cfg(not(feature = "active"))]
        {
            _ = task_id;
            _ = lock_id;
        }
        #[cfg(feature = "active")]
        {
            let waiting = self
                .waiting
                .get_mut(&task_id)
                .ok_or_else(|| BadSync("the task is not waiting for the mutex".to_string()))?;
            if !waiting.remove(&lock_id) {
                return Err(BadSync("the task is not waiting for the mutex".to_string()));
            }
            if waiting.is_empty() {
                self.waiting.remove(&task_id);
            }
            self.held_by.insert(lock_id, task_id);
        }
        Ok(NotificationOutcome::Acknowledged)
    }
}

impl ProcessSyncEvent<ReleasedMutex> for MutexModel {
    fn on_event(
        &mut self,
        task_id: TaskId,
        ReleasedMutex(lock_id): ReleasedMutex,
    ) -> Result<NotificationOutcome, BadSync> {
        #[cfg(not(feature = "active"))]
        {
            _ = task_id;
            _ = lock_id;
        }
        #[cfg(feature = "active")]
        {
            if self.held_by.remove(&lock_id) != Some(task_id) {
                return Err(BadSync("the task was not holding the lock".to_string()));
            }
        }
        Ok(NotificationOutcome::Acknowledged)
    }
}
