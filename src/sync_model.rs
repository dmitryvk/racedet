//! Task scheduler needs the knowledge of task synchronization, such as locks.
//! Without that information, the following situation is possible:
//! - Task A holds lock X
//! - Task B tries to take lock X
//! - Scheduler B picks the task B as runnable
//! - Since that task can not make progress while that lock is held, the whole execution halts.
//!
//! To handle the situation well, at each schedule opportunity the scheduler needs to know if
//! the task can make progress.
//! This requires modeling synchronization primitives such as locks or joins.
//! Synchronization operations are represented by structs implementing `SyncEvent` trait.
//! This allows for extensibility and for supporting synchronization operations with various semantics, e.g.
//! recursive vs non-recursive mutexes, fair and non-fair mutexes, in-memory and SQL locks.
//! `SyncEvent` implementations are backed by `SyncModel` which tracks the necessary state
//! (e.g., which tasks are holding locks or trying to acquire them).

use std::{
    any::{Any, TypeId},
    collections::{HashMap, HashSet},
};

use crate::TaskId;

pub mod join;
pub mod mutex;
pub mod rwlock;
pub mod start_barrier;

pub(crate) struct SyncModelRegistry {
    sync_models: HashMap<TypeId, Box<dyn DynSyncModel>>,
}

impl SyncModelRegistry {
    #[allow(clippy::new_without_default)]
    pub(crate) fn new() -> Self {
        Self {
            sync_models: HashMap::new(),
        }
    }

    fn get_sync_model_mut<T: SyncModel>(&mut self) -> &mut T {
        let dyn_model = self
            .sync_models
            .entry(TypeId::of::<T>())
            .or_insert_with(|| Box::new(T::default()));
        let any: &mut dyn Any = dyn_model.as_mut();
        any.downcast_mut().expect("used the matching TypeId")
    }

    fn iter_models(&self) -> impl Iterator<Item = &dyn DynSyncModel> {
        self.sync_models.values().map(|model| model.as_ref())
    }

    pub(crate) fn on_notified<TEvent: SyncEvent>(
        &mut self,
        task_id: TaskId,
        event: TEvent,
    ) -> Result<NotificationOutcome, BadSyncError> {
        let sync_model = self.get_sync_model_mut::<TEvent::Model>();
        sync_model.on_notified(task_id, event)
    }

    pub(crate) fn on_init_event<TEvent: SyncInitEvent>(
        &mut self,
        event: TEvent,
    ) -> Result<NotificationOutcome, BadSyncError> {
        let sync_model = self.get_sync_model_mut::<TEvent::Model>();
        sync_model.on_init_event(event)
    }

    pub(crate) fn task_progress_dependencies(&self, task_id: TaskId) -> TaskProgressDependencies {
        self.iter_models()
            .map(|model| model.task_progress_dependencies(task_id))
            .reduce(TaskProgressDependencies::merge)
            .unwrap_or_else(|| TaskProgressDependencies::Ready {
                need_to_run: HashSet::new(),
            })
    }
}

pub trait DynSyncModel: Any + Send + Sync + 'static {
    fn task_progress_dependencies(&self, task_id: TaskId) -> TaskProgressDependencies;
}

pub trait SyncModel: DynSyncModel + Default {}

pub trait ProcessSyncEvent<TOp: SyncEvent>: SyncModel {
    fn on_notified(
        &mut self,
        task_id: TaskId,
        event: TOp,
    ) -> Result<NotificationOutcome, BadSyncError>;
}

pub trait ProcessSyncInitEvent<TOp: SyncInitEvent>: SyncModel {
    fn on_init_event(&mut self, event: TOp) -> Result<NotificationOutcome, BadSyncError>;
}

pub trait SyncInitEvent: Sized {
    type Model: ProcessSyncInitEvent<Self>;
}

pub trait SyncEvent: Sized {
    type Model: ProcessSyncEvent<Self>;
}

#[derive(Debug, PartialEq, Eq)]
pub enum NotificationOutcome {
    Acknowledged,
    // A re-schedule may be required if a task becomes blocking without reaching suspension point
    ScheduleRequired,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TaskProgressDependencies {
    /// The task is ready to be resumed, but it needs other tasks to be running at the same time (due to being driven by them or a cooperative concurrency/synchronization primitive)
    /// Examples:
    /// - mutex lock that is non-locked (depends on no other tasks)
    /// - barrier with sufficient number of waiters (depends on other waiters for the same barrier)
    /// - task inside of tokio::join or BufferedUnordered (depends on the parent task)
    /// - waiting on tokio::watch if not waiting for recheck
    ///
    /// Note:
    /// `Ready` may specify `Blocked` task in `need_to_run`. E.g., a task inside of `tokio::join` will specify the parent as `need_to_run` while parent will be `Blocked`
    Ready { need_to_run: HashSet<TaskId> },
    /// The task is blocked on a synchronization primitive and cannot proceed yet (i.e., it will not reach its next suspension point).
    /// If the task is suspended, it will not be selected for resumption unless other task requires it.
    /// If all running tasks are `Blocked`, the scheduler will add additional `Ready` tasks.
    /// If all tasks are `Blocked`, there is either a deadlock (if external tasks are impossible) or just not enough tasks are started (if external tasks are impossible).
    /// Examples:
    /// - tokio::watch if recheck is required (and was not updated)
    /// - tokio::join/BufferedUnordered/tokio::spawn
    /// - barrier with insufficient waiters
    // TODO: include the blocking tasks for deadlock detection
    Blocked,
}

impl TaskProgressDependencies {
    fn merge(self, other: Self) -> Self {
        match (self, other) {
            (
                Self::Ready {
                    need_to_run: mut t1,
                },
                Self::Ready { need_to_run: t2 },
            ) => Self::Ready {
                need_to_run: {
                    t1.extend(t2);
                    t1
                },
            },
            (Self::Blocked, Self::Ready { .. } | Self::Blocked)
            | (Self::Ready { .. }, Self::Blocked) => Self::Blocked,
        }
    }
}

#[derive(Debug)]
pub struct BadSyncError(String);

impl std::fmt::Display for BadSyncError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "bad use of SyncModel: {}", self.0)
    }
}

impl std::error::Error for BadSyncError {}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use super::*;

    #[derive(Debug, Clone, Copy, Hash, Eq, PartialEq)]
    struct LockId(u32);

    #[derive(Default)]
    struct LockModel {
        locks_held_by: HashMap<LockId, TaskId>,
        locks_waiting: HashMap<TaskId, LockId>,
    }

    struct TakingLock(LockId);
    impl SyncEvent for TakingLock {
        type Model = LockModel;
    }
    struct AbortTakingLock(LockId);
    impl SyncEvent for AbortTakingLock {
        type Model = LockModel;
    }
    struct LockTaken(LockId);
    impl SyncEvent for LockTaken {
        type Model = LockModel;
    }
    struct LockReleased(LockId);
    impl SyncEvent for LockReleased {
        type Model = LockModel;
    }

    impl SyncModel for LockModel {}
    impl DynSyncModel for LockModel {
        fn task_progress_dependencies(&self, task_id: TaskId) -> TaskProgressDependencies {
            let Some(lock_id) = self.locks_waiting.get(&task_id).cloned() else {
                return TaskProgressDependencies::Ready {
                    need_to_run: HashSet::new(),
                };
            };
            let Some(holding_task_id) = self.locks_held_by.get(&lock_id).cloned() else {
                return TaskProgressDependencies::Ready {
                    need_to_run: HashSet::new(),
                };
            };

            TaskProgressDependencies::Ready {
                need_to_run: [holding_task_id].into_iter().collect(),
            }
        }
    }

    impl ProcessSyncEvent<TakingLock> for LockModel {
        fn on_notified(
            &mut self,
            task_id: TaskId,
            TakingLock(lock_id): TakingLock,
        ) -> Result<NotificationOutcome, BadSyncError> {
            if self.locks_waiting.contains_key(&task_id) {
                return Err(BadSyncError("task already waiting for a lock".to_string()));
            }
            if self.locks_held_by.get(&lock_id) == Some(&task_id) {
                return Err(BadSyncError(
                    "the task is already holding the lock".to_string(),
                ));
            }
            self.locks_waiting.insert(task_id, lock_id);
            Ok(NotificationOutcome::Acknowledged)
        }
    }
    impl ProcessSyncEvent<AbortTakingLock> for LockModel {
        fn on_notified(
            &mut self,
            task_id: TaskId,
            AbortTakingLock(lock_id): AbortTakingLock,
        ) -> Result<NotificationOutcome, BadSyncError> {
            if self.locks_waiting.remove(&task_id) != Some(lock_id) {
                return Err(BadSyncError(
                    "the task is not waiting for the lock".to_string(),
                ));
            }
            Ok(NotificationOutcome::Acknowledged)
        }
    }
    impl ProcessSyncEvent<LockTaken> for LockModel {
        fn on_notified(
            &mut self,
            task_id: TaskId,
            LockTaken(lock_id): LockTaken,
        ) -> Result<NotificationOutcome, BadSyncError> {
            if self.locks_held_by.contains_key(&lock_id) {
                return Err(BadSyncError("lock is already taken".to_string()));
            }
            if self.locks_waiting.remove(&task_id) != Some(lock_id) {
                return Err(BadSyncError("task is not waiting for a lock".to_string()));
            }
            self.locks_held_by.insert(lock_id, task_id);
            Ok(NotificationOutcome::Acknowledged)
        }
    }
    impl ProcessSyncEvent<LockReleased> for LockModel {
        fn on_notified(
            &mut self,
            task_id: TaskId,
            LockReleased(lock_id): LockReleased,
        ) -> Result<NotificationOutcome, BadSyncError> {
            if self.locks_held_by.remove(&lock_id) != Some(task_id) {
                return Err(BadSyncError(
                    "the lock was not taken by the task".to_string(),
                ));
            }
            Ok(NotificationOutcome::Acknowledged)
        }
    }

    #[test]
    fn test_lock_model() {
        let mut registry = SyncModelRegistry::new();
        let task_1 = TaskId(NonZeroU64::new(1).unwrap());
        let task_2 = TaskId(NonZeroU64::new(2).unwrap());
        let lock_id = LockId(1);
        assert_eq!(
            registry.task_progress_dependencies(task_1),
            TaskProgressDependencies::Ready {
                need_to_run: HashSet::new()
            }
        );
        assert_eq!(
            registry.task_progress_dependencies(task_2),
            TaskProgressDependencies::Ready {
                need_to_run: HashSet::new()
            }
        );

        assert_eq!(
            registry.on_notified(task_1, TakingLock(lock_id)).unwrap(),
            NotificationOutcome::Acknowledged
        );
        registry
            .on_notified(task_1, TakingLock(LockId(2)))
            .unwrap_err();
        assert_eq!(
            registry.task_progress_dependencies(task_1),
            TaskProgressDependencies::Ready {
                need_to_run: HashSet::new()
            }
        );
        assert_eq!(
            registry.task_progress_dependencies(task_2),
            TaskProgressDependencies::Ready {
                need_to_run: HashSet::new()
            }
        );

        assert_eq!(
            registry.on_notified(task_1, LockTaken(lock_id)).unwrap(),
            NotificationOutcome::Acknowledged
        );
        assert_eq!(
            registry.task_progress_dependencies(task_1),
            TaskProgressDependencies::Ready {
                need_to_run: HashSet::new()
            },
        );
        assert_eq!(
            registry.task_progress_dependencies(task_2),
            TaskProgressDependencies::Ready {
                need_to_run: HashSet::new()
            }
        );

        assert_eq!(
            registry.on_notified(task_2, TakingLock(lock_id)).unwrap(),
            NotificationOutcome::Acknowledged
        );
        assert_eq!(
            registry.task_progress_dependencies(task_1),
            TaskProgressDependencies::Ready {
                need_to_run: HashSet::new()
            },
        );
        assert_eq!(
            registry.task_progress_dependencies(task_2),
            TaskProgressDependencies::Ready {
                need_to_run: [task_1].into_iter().collect()
            },
        );

        let err = registry
            .on_notified(task_2, LockTaken(lock_id))
            .unwrap_err();
        assert!(err.0.contains("already taken"), "returned error: {err}");
        assert_eq!(
            registry.task_progress_dependencies(task_1),
            TaskProgressDependencies::Ready {
                need_to_run: HashSet::new()
            },
        );
        assert_eq!(
            registry.task_progress_dependencies(task_2),
            TaskProgressDependencies::Ready {
                need_to_run: [task_1].into_iter().collect()
            },
        );

        assert_eq!(
            registry.on_notified(task_1, LockReleased(lock_id)).unwrap(),
            NotificationOutcome::Acknowledged
        );
        assert_eq!(
            registry.task_progress_dependencies(task_1),
            TaskProgressDependencies::Ready {
                need_to_run: HashSet::new()
            },
        );
        assert_eq!(
            registry.task_progress_dependencies(task_2),
            TaskProgressDependencies::Ready {
                need_to_run: HashSet::new()
            },
        );

        assert_eq!(
            registry.on_notified(task_2, LockTaken(lock_id)).unwrap(),
            NotificationOutcome::Acknowledged
        );
        assert_eq!(
            registry.task_progress_dependencies(task_1),
            TaskProgressDependencies::Ready {
                need_to_run: HashSet::new()
            },
        );
        assert_eq!(
            registry.task_progress_dependencies(task_2),
            TaskProgressDependencies::Ready {
                need_to_run: HashSet::new()
            },
        );

        assert_eq!(
            registry.on_notified(task_2, LockReleased(lock_id)).unwrap(),
            NotificationOutcome::Acknowledged
        );
        assert_eq!(
            registry.task_progress_dependencies(task_1),
            TaskProgressDependencies::Ready {
                need_to_run: HashSet::new()
            },
        );
        assert_eq!(
            registry.task_progress_dependencies(task_2),
            TaskProgressDependencies::Ready {
                need_to_run: HashSet::new()
            },
        );

        assert_eq!(
            registry.on_notified(task_2, TakingLock(lock_id)).unwrap(),
            NotificationOutcome::Acknowledged
        );
        assert_eq!(
            registry
                .on_notified(task_2, AbortTakingLock(lock_id))
                .unwrap(),
            NotificationOutcome::Acknowledged
        );
        assert_eq!(
            registry.task_progress_dependencies(task_1),
            TaskProgressDependencies::Ready {
                need_to_run: HashSet::new()
            }
        );
        assert_eq!(
            registry.task_progress_dependencies(task_2),
            TaskProgressDependencies::Ready {
                need_to_run: HashSet::new()
            }
        );

        assert_eq!(
            registry.on_notified(task_1, TakingLock(lock_id)).unwrap(),
            NotificationOutcome::Acknowledged
        );
        assert_eq!(
            registry.task_progress_dependencies(task_1),
            TaskProgressDependencies::Ready {
                need_to_run: HashSet::new()
            }
        );
        assert_eq!(
            registry.task_progress_dependencies(task_2),
            TaskProgressDependencies::Ready {
                need_to_run: HashSet::new()
            }
        );
    }
}
