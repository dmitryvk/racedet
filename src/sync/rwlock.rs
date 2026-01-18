use std::collections::{HashMap, HashSet};

use crate::{
    sync::{
        BadSync, DynSyncModel, NotificationOutcome, ProcessSyncEvent, SyncEvent, SyncModel,
        TaskProgressDependencies,
    },
    task::TaskId,
};

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct RwlockId(String);
impl RwlockId {
    pub fn new(id: String) -> Self {
        Self(id)
    }
}

#[derive(Default)]
pub struct RwlockModel {
    held_by: HashMap<RwlockId, HeldRwlock>,
    waiting: HashMap<TaskId, HashSet<(RwlockId, RwlockMode)>>,
}

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
pub enum RwlockMode {
    Read,
    Write,
}

#[derive(Debug)]
enum HeldRwlock {
    Read(HashSet<TaskId>),
    Write(TaskId),
}

impl SyncModel for RwlockModel {}
impl DynSyncModel for RwlockModel {
    fn task_progress_dependencies(&self, task_id: TaskId) -> TaskProgressDependencies {
        let Some(waiting) = self.waiting.get(&task_id) else {
            return TaskProgressDependencies::Ready {
                need_to_run: HashSet::new(),
            };
        };
        let held_by: HashSet<TaskId> = waiting
            .iter()
            .flat_map(
                |(mutex_id, lock_mode)| match (lock_mode, self.held_by.get(mutex_id)) {
                    (_, None) => HashSet::new(),
                    (RwlockMode::Read, Some(HeldRwlock::Read(_))) => HashSet::new(),
                    (RwlockMode::Read | RwlockMode::Write, Some(HeldRwlock::Write(task_id))) => {
                        [*task_id].into_iter().collect()
                    }
                    (RwlockMode::Write, Some(HeldRwlock::Read(task_ids))) => task_ids.clone(),
                },
            )
            .collect();
        TaskProgressDependencies::Ready {
            need_to_run: held_by,
        }
    }
}

pub struct LockingRwlock(pub RwlockId, pub RwlockMode);
impl SyncEvent for LockingRwlock {
    type Model = RwlockModel;
}
pub struct AbortedLockingRwlock(pub RwlockId, pub RwlockMode);
impl SyncEvent for AbortedLockingRwlock {
    type Model = RwlockModel;
}
pub struct LockedRwlock(pub RwlockId, pub RwlockMode);
impl SyncEvent for LockedRwlock {
    type Model = RwlockModel;
}
pub struct ReleasedRwlock(pub RwlockId, pub RwlockMode);
impl SyncEvent for ReleasedRwlock {
    type Model = RwlockModel;
}

impl ProcessSyncEvent<LockingRwlock> for RwlockModel {
    fn on_event(
        &mut self,
        task_id: TaskId,
        LockingRwlock(lock_id, lock_mode): LockingRwlock,
    ) -> Result<NotificationOutcome, BadSync> {
        self.waiting
            .entry(task_id)
            .or_default()
            .insert((lock_id, lock_mode));
        Ok(NotificationOutcome::Acknowledged)
    }
}

impl ProcessSyncEvent<AbortedLockingRwlock> for RwlockModel {
    fn on_event(
        &mut self,
        task_id: TaskId,
        AbortedLockingRwlock(lock_id, lock_mode): AbortedLockingRwlock,
    ) -> Result<NotificationOutcome, BadSync> {
        let waiting = self
            .waiting
            .get_mut(&task_id)
            .ok_or_else(|| BadSync("the task is not waiting for the mutex".to_string()))?;
        if !waiting.remove(&(lock_id, lock_mode)) {
            return Err(BadSync("the task is not waiting for the mutex".to_string()));
        }
        if waiting.is_empty() {
            self.waiting.remove(&task_id);
        }
        Ok(NotificationOutcome::Acknowledged)
    }
}

impl ProcessSyncEvent<LockedRwlock> for RwlockModel {
    fn on_event(
        &mut self,
        task_id: TaskId,
        LockedRwlock(lock_id, lock_mode): LockedRwlock,
    ) -> Result<NotificationOutcome, BadSync> {
        let waiting = self
            .waiting
            .get_mut(&task_id)
            .ok_or_else(|| BadSync("the task is not waiting for the mutex".to_string()))?;
        if !waiting.remove(&(lock_id.clone(), lock_mode)) {
            return Err(BadSync("the task is not waiting for the mutex".to_string()));
        }
        if waiting.is_empty() {
            self.waiting.remove(&task_id);
        }
        match lock_mode {
            RwlockMode::Read => {
                match self
                    .held_by
                    .entry(lock_id)
                    .or_insert_with(|| HeldRwlock::Read(HashSet::new()))
                {
                    HeldRwlock::Read(held) => {
                        held.insert(task_id);
                    }
                    HeldRwlock::Write(_) => {
                        return Err(BadSync("the lock is already held".to_string()));
                    }
                }
            }
            RwlockMode::Write => {
                self.held_by.insert(lock_id, HeldRwlock::Write(task_id));
            }
        }
        Ok(NotificationOutcome::Acknowledged)
    }
}

impl ProcessSyncEvent<ReleasedRwlock> for RwlockModel {
    fn on_event(
        &mut self,
        task_id: TaskId,
        ReleasedRwlock(lock_id, lock_mode): ReleasedRwlock,
    ) -> Result<NotificationOutcome, BadSync> {
        match lock_mode {
            RwlockMode::Read => {
                let held_by = self
                    .held_by
                    .get_mut(&lock_id)
                    .ok_or_else(|| BadSync("the task was not holding the lock".to_string()))?;
                match held_by {
                    HeldRwlock::Read(hash_set) => {
                        hash_set.remove(&task_id);
                        if hash_set.is_empty() {
                            self.held_by.remove(&lock_id);
                        }
                    }
                    HeldRwlock::Write(_) => {
                        return Err(BadSync("the lock is held as write lock".to_string()));
                    }
                }
            }
            RwlockMode::Write => {
                self.held_by.remove(&lock_id);
            }
        }
        Ok(NotificationOutcome::Acknowledged)
    }
}
