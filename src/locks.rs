//! Task scheduler needs the knowledge of task synchronization, such as locks.
//! Without that information, the following situation is possible:
//! - Task A holds lock X
//! - Task B tries to take lock X
//! - Scheduler B picks the task B as runnable
//! - Since that task can not make progress while that lock is held, the whole execution halts.
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

pub struct ErasedLocks {
    inner: HashMap<TypeId, Box<dyn Any>>,
}

impl ErasedLocks {
    pub fn new() -> Self {
        Self {
            inner: HashMap::new(),
        }
    }

    fn get<T: SyncDomain>(&mut self) -> &mut T {
        let type_id = TypeId::of::<T>();
        self.inner
            .entry(type_id)
            .or_insert_with(|| Box::new(T::default()))
            .downcast_mut()
            .expect("the key is TypeId of T")
    }

    pub fn is_task_runnable<D, L>(&mut self, task_id: TaskId, operation: &L) -> bool
    where
        D: SyncDomain<SyncOperation = L>,
        L: SyncOperation<Domain = D>,
    {
        self.get::<D>().is_task_runnable(task_id, operation)
    }

    pub fn task_selected_for_running<D, L>(
        &mut self,
        task_id: TaskId,
        operation: &L,
    ) -> Result<(), BadSyncError>
    where
        D: SyncDomain<SyncOperation = L>,
        L: SyncOperation<Domain = D>,
    {
        self.get::<D>()
            .task_selected_for_running(task_id, operation)
    }
}

/// A stateful model of semantics of locks.
/// It is used as follows:
/// - Each suspend point carries an (optional) `SyncOperation` with it.
/// - When scheduler picks a task, it:
///   - calls `is_task_runnable` to determine if the task will make progress with the sync operation.
///   - calls `task_selected_for_running` to notify which task is selected as running (only if `is_task_runnable` returned true for this task).
///     `SyncDomain` should assume that the requested sync operation is completed
pub trait SyncOperation: Default + Send + 'static {
    type SyncOperation: SyncOperation<Domain = Self> + Send + 'static;
    fn is_task_runnable(&self, task: TaskId, op: &Self::SyncOperation) -> bool;
    fn task_selected_for_running(
        &mut self,
        task: TaskId,
        op: &Self::SyncOperation,
    ) -> Result<(), BadSyncError>;
}

pub trait SyncOperation {
    type Domain: SyncDomain<SyncOperation = Self>;
}

#[derive(Debug)]
pub struct BadSyncError(String);

impl std::fmt::Display for BadSyncError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "bad use of SyncDomain: {}", self.0)
    }
}

impl std::error::Error for BadSyncError {}

#[derive(Default)]
struct InMemoryLocks {
    lock_held_by: HashMap<String, TaskId>,
}

pub enum LockOperation {
    Acquire(String),
    Release(String),
}

impl SyncOperation for LockOperation {
    type State = InMemoryLocks;
}

impl SyncDomain for InMemoryLocks {
    type SyncOperation = LockOperation;

    fn is_task_runnable(&self, _task: TaskId, op: &Self::SyncOperation) -> bool {
        match op {
            LockOperation::Acquire(lock_id) => !self.lock_held_by.contains_key(lock_id),
            LockOperation::Release(_) => true,
        }
    }

    fn task_selected_for_running(
        &mut self,
        task: TaskId,
        op: &Self::SyncOperation,
    ) -> Result<(), BadSyncError> {
        match op {
            LockOperation::Acquire(lock_id) => {
                let was_held_by = self.lock_held_by.insert(lock_id.clone(), task);
                if let Some(was_held_by) = was_held_by {
                    Err(BadSyncError(format!("lock is held by task {was_held_by}")))
                } else {
                    Ok(())
                }
            }
            LockOperation::Release(lock_id) => {
                let was_held_by = self.lock_held_by.remove(lock_id);
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

trait LockInDomain {}
