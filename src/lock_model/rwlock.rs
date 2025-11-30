use std::collections::HashMap;

use crate::{SyncOperation, TaskId, lock_model::BadSyncError};

#[derive(Default)]
pub struct RwlockState {
    lock_held_by: HashMap<String, HeldRwlock>,
}

#[derive(Debug)]
enum HeldRwlock {
    Read(Vec<TaskId>),
    Write(TaskId),
}

#[derive(Debug)]
pub enum RwlockOperation {
    AcquireRead(String),
    AcquireWrite(String),
    Release(String),
}

impl SyncOperation for RwlockOperation {
    type State = RwlockState;

    fn is_task_runnable(&self, state: &Self::State, _task: TaskId) -> bool {
        match self {
            RwlockOperation::AcquireRead(lock_id) => state
                .lock_held_by
                .get(lock_id)
                .is_none_or(|held| matches!(held, HeldRwlock::Read(_))),
            RwlockOperation::AcquireWrite(lock_id) => !state.lock_held_by.contains_key(lock_id),
            RwlockOperation::Release(_) => true,
        }
    }

    fn task_selected_for_running(
        &self,
        state: &mut Self::State,
        task: TaskId,
    ) -> Result<(), BadSyncError> {
        use std::collections::hash_map::Entry;
        match self {
            RwlockOperation::AcquireRead(lock_id) => {
                match state.lock_held_by.entry(lock_id.clone()) {
                    Entry::Occupied(mut entry) => match entry.get_mut() {
                        HeldRwlock::Read(tasks) => {
                            tasks.push(task);
                            Ok(())
                        }
                        HeldRwlock::Write(held_by) => {
                            Err(BadSyncError(format!("rwlock is held by {held_by}")))
                        }
                    },
                    Entry::Vacant(entry) => {
                        entry.insert(HeldRwlock::Read(vec![task]));
                        Ok(())
                    }
                }
            }
            RwlockOperation::AcquireWrite(lock_id) => {
                match state.lock_held_by.entry(lock_id.clone()) {
                    Entry::Occupied(entry) => {
                        Err(BadSyncError(format!("rwlock is held by {entry:?}")))
                    }
                    Entry::Vacant(entry) => {
                        entry.insert(HeldRwlock::Write(task));
                        Ok(())
                    }
                }
            }
            RwlockOperation::Release(lock_id) => match state.lock_held_by.entry(lock_id.clone()) {
                Entry::Occupied(mut entry) => match entry.get_mut() {
                    HeldRwlock::Read(tasks) => {
                        let Some(idx) = tasks.iter().position(|item| *item == task) else {
                            return Err(BadSyncError(format!(
                                "rwlock is not held by {task}: {tasks:?}"
                            )));
                        };
                        tasks.remove(idx);
                        if tasks.is_empty() {
                            entry.remove();
                        }
                        Ok(())
                    }
                    HeldRwlock::Write(held_by) => {
                        if *held_by == task {
                            entry.remove();
                            Ok(())
                        } else {
                            Err(BadSyncError(format!("lock is held by {held_by}")))
                        }
                    }
                },
                Entry::Vacant(entry) => {
                    entry.insert(HeldRwlock::Read(vec![task]));
                    Ok(())
                }
            },
        }
    }
}
