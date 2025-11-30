//! Task scheduler needs the knowledge of task synchronization, such as locks.
//! Without that information, the following situation is possible:
//! - Task A holds lock X
//! - Task B tries to take lock X
//! - Scheduler B picks the task B as runnable
//! - Since that task can not make progress while that lock is held, the whole execution halts.
//!
//! To handle the situation well, at each suspend point the scheduler needs to know if
//! the task can make progress.
//! This requires some level of modeling locks and other synchronization operations between tasks.
//! Synchronization operations are represented by structs implementing `SyncOperation` trait.
//! This allows for extensibility and for supporting synchronization operations with different semantics, e.g.
//! recursive vs non-recursive mutexes, fair and non-fair mutexes, in-memory and SQL locks.
//! `SyncOperation` implementations are backed by `SyncDomain` which tracks the necessary state
//! (e.g., which tasks are holding locks or trying to acquire them).

use std::{
    any::{Any, TypeId},
    collections::HashMap,
};

use crate::TaskId;

pub struct ErasedSyncState {
    lock_states: HashMap<TypeId, Box<dyn Any + Sync + Send>>,
}

impl ErasedSyncState {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Self {
            lock_states: HashMap::new(),
        }
    }

    pub fn make_dyn_operation<T: SyncOperation>(&mut self, operation: T) -> BoxDynSyncOperation {
        let type_id = TypeId::of::<T::State>();
        _ = self
            .lock_states
            .entry(type_id)
            .or_insert_with(|| Box::new(T::State::default()));

        BoxDynSyncOperation {
            state_type: type_id,
            operation: Box::new(operation),
        }
    }

    fn get_state(&self, type_id: TypeId) -> &dyn Any {
        self.lock_states
            .get(&type_id)
            .expect("did not find lock state")
            .as_ref()
    }

    fn get_state_mut(&mut self, type_id: TypeId) -> &mut dyn Any {
        self.lock_states
            .get_mut(&type_id)
            .expect("did not find lock state")
            .as_mut()
    }

    pub fn is_task_runnable(&self, task_id: TaskId, operation: &BoxDynSyncOperation) -> bool {
        let state = self.get_state(operation.state_type);
        operation.operation.is_task_runnable(state, task_id)
    }

    pub fn task_selected_for_running(
        &mut self,
        task_id: TaskId,
        operation: &BoxDynSyncOperation,
    ) -> Result<(), BadSyncError> {
        let state = self.get_state_mut(operation.state_type);
        operation
            .operation
            .task_selected_for_running(state, task_id)
    }
}

/// A stateful model of semantics of locks.
/// It is used as follows:
/// - Each suspend point carries an (optional) `SyncOperation` with it.
/// - When scheduler picks a task, it:
///   - calls `is_task_runnable` to determine if the task will make progress with the sync operation.
///   - calls `task_selected_for_running` to notify which task is selected as running (only if `is_task_runnable` returned true for this task).
///     `SyncDomain` should assume that the requested sync operation is completed
pub trait SyncOperation: std::fmt::Debug + Sync + Send + 'static {
    type State: Default + Sync + Send + 'static;
    fn is_task_runnable(&self, state: &Self::State, task: TaskId) -> bool;
    fn task_selected_for_running(
        &self,
        state: &mut Self::State,
        task: TaskId,
    ) -> Result<(), BadSyncError>;
}

#[derive(Debug)]
pub struct BoxDynSyncOperation {
    state_type: TypeId,
    operation: Box<dyn DynSyncOperation + Sync + Send>,
}

pub trait DynSyncOperation: std::fmt::Debug {
    fn is_task_runnable(&self, state: &dyn Any, task: TaskId) -> bool;
    fn task_selected_for_running(
        &self,
        state: &mut dyn Any,
        task: TaskId,
    ) -> Result<(), BadSyncError>;
}

impl<T> DynSyncOperation for T
where
    T: SyncOperation,
{
    fn is_task_runnable(&self, state: &dyn Any, task: TaskId) -> bool {
        let state = state
            .downcast_ref::<T::State>()
            .expect("should be called with a compatible state");
        SyncOperation::is_task_runnable(self, state, task)
    }

    fn task_selected_for_running(
        &self,
        state: &mut dyn Any,
        task: TaskId,
    ) -> Result<(), BadSyncError> {
        let state = state
            .downcast_mut::<T::State>()
            .expect("should be called with a compatible state");
        SyncOperation::task_selected_for_running(self, state, task)
    }
}

#[derive(Debug)]
pub struct BadSyncError(String);

impl std::fmt::Display for BadSyncError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "bad use of SyncDomain: {}", self.0)
    }
}

impl std::error::Error for BadSyncError {}

#[derive(Debug)]
pub struct Noop;

impl SyncOperation for Noop {
    type State = ();

    fn is_task_runnable(&self, _state: &Self::State, _task: TaskId) -> bool {
        true
    }

    fn task_selected_for_running(
        &self,
        _state: &mut Self::State,
        _task: TaskId,
    ) -> Result<(), BadSyncError> {
        Ok(())
    }
}

#[derive(Default)]
pub struct InMemoryLocks {
    lock_held_by: HashMap<String, TaskId>,
}

#[derive(Debug)]
pub enum LockOperation {
    Acquire(String),
    Release(String),
}

impl SyncOperation for LockOperation {
    type State = InMemoryLocks;

    fn is_task_runnable(&self, state: &Self::State, _task: TaskId) -> bool {
        match self {
            LockOperation::Acquire(lock_id) => !state.lock_held_by.contains_key(lock_id),
            LockOperation::Release(_) => true,
        }
    }

    fn task_selected_for_running(
        &self,
        state: &mut Self::State,
        task: TaskId,
    ) -> Result<(), BadSyncError> {
        match self {
            LockOperation::Acquire(lock_id) => {
                let was_held_by = state.lock_held_by.insert(lock_id.clone(), task);
                if let Some(was_held_by) = was_held_by {
                    Err(BadSyncError(format!("lock is held by task {was_held_by}")))
                } else {
                    Ok(())
                }
            }
            LockOperation::Release(lock_id) => {
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
