use std::collections::HashMap;

use crate::{SyncOperation, TaskId, lock_model::BadSyncError};

#[derive(Default)]
pub struct MutexState {
    lock_held_by: HashMap<String, TaskId>,
}

#[derive(Debug)]
pub enum MutexOperation {
    Acquire(String),
    Release(String),
}

impl SyncOperation for MutexOperation {
    type State = MutexState;

    fn is_task_runnable(&self, state: &Self::State, _task: TaskId) -> bool {
        match self {
            MutexOperation::Acquire(lock_id) => !state.lock_held_by.contains_key(lock_id),
            MutexOperation::Release(_) => true,
        }
    }

    fn task_selected_for_running(
        &self,
        state: &mut Self::State,
        task: TaskId,
    ) -> Result<(), BadSyncError> {
        match self {
            MutexOperation::Acquire(lock_id) => {
                let was_held_by = state.lock_held_by.insert(lock_id.clone(), task);
                if let Some(was_held_by) = was_held_by {
                    Err(BadSyncError(format!("lock is held by task {was_held_by}")))
                } else {
                    Ok(())
                }
            }
            MutexOperation::Release(lock_id) => {
                let was_held_by = state.lock_held_by.remove(lock_id);
                if was_held_by != Some(task) {
                    Err(BadSyncError(format!(
                        "lock was not held by {task} but rather {was_held_by:?}"
                    )))
                } else {
                    Ok(())
                }
            }
        }
    }
}
