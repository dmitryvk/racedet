use std::{
    any::{Any, TypeId},
    collections::{HashMap, HashSet},
};

use crate::TaskId;

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

    pub(crate) fn task_progress_dependencies(&self, task_id: TaskId) -> TaskProgressDependencies {
        self.iter_models()
            .map(|model| model.task_progress_dependencies(task_id))
            .reduce(TaskProgressDependencies::merge)
            .unwrap_or_else(|| TaskProgressDependencies::Known(HashSet::new()))
    }
}

trait DynSyncModel: Any + Send + Sync + 'static {
    fn task_progress_dependencies(&self, task_id: TaskId) -> TaskProgressDependencies;
}

trait SyncModel: DynSyncModel + Default {}

trait ProcessSyncEvent<TOp: SyncEvent>: SyncModel {
    fn on_notified(
        &mut self,
        task_id: TaskId,
        event: TOp,
    ) -> Result<NotificationOutcome, BadSyncError>;
}

trait SyncEvent: Sized {
    type Model: ProcessSyncEvent<Self>;
}

#[derive(Debug, PartialEq, Eq)]
enum NotificationOutcome {
    Acknowledged,
    ScheduleRequired,
}

#[derive(Debug, PartialEq, Eq)]
enum TaskProgressDependencies {
    // if the specified tasks are running, this task is guaranteed to make progress (e.g. a mutex or a barrier with a necessary amount of waiters)
    Known(HashSet<TaskId>),
    // the current task is waiting, and we don't know which task will unblock it; the task must be running in order to catch that event; and the scheduler should keep some other task(s) running concurrently (e.g., a condvar or join)
    UnknownActive,
    // the current task is waiting, and we don't know which task will unblock it; the task does not need to be resumed, as the sync model will know when to resume it (e.g., a barrier with not enough waiters)
    UnknownPassive,
}

impl TaskProgressDependencies {
    fn merge(self, other: Self) -> Self {
        match (self, other) {
            (Self::Known(mut t1), Self::Known(t2)) => Self::Known({
                t1.extend(t2.into_iter());
                t1
            }),
            (Self::UnknownActive, Self::Known(_) | Self::UnknownActive | Self::UnknownPassive)
            | (Self::Known(_) | Self::UnknownPassive, Self::UnknownActive) => Self::UnknownActive,
            (Self::UnknownPassive, Self::Known(_) | Self::UnknownPassive)
            | (Self::Known(_), Self::UnknownPassive) => Self::UnknownPassive,
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
                return TaskProgressDependencies::Known(HashSet::new());
            };
            let Some(holding_task_id) = self.locks_held_by.get(&lock_id).cloned() else {
                return TaskProgressDependencies::Known(HashSet::new());
            };

            TaskProgressDependencies::Known([holding_task_id].into_iter().collect())
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
            TaskProgressDependencies::Known(HashSet::new())
        );
        assert_eq!(
            registry.task_progress_dependencies(task_2),
            TaskProgressDependencies::Known(HashSet::new())
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
            TaskProgressDependencies::Known(HashSet::new())
        );
        assert_eq!(
            registry.task_progress_dependencies(task_2),
            TaskProgressDependencies::Known(HashSet::new())
        );

        assert_eq!(
            registry.on_notified(task_1, LockTaken(lock_id)).unwrap(),
            NotificationOutcome::Acknowledged
        );
        assert_eq!(
            registry.task_progress_dependencies(task_1),
            TaskProgressDependencies::Known(HashSet::new()),
        );
        assert_eq!(
            registry.task_progress_dependencies(task_2),
            TaskProgressDependencies::Known(HashSet::new())
        );

        assert_eq!(
            registry.on_notified(task_2, TakingLock(lock_id)).unwrap(),
            NotificationOutcome::Acknowledged
        );
        assert_eq!(
            registry.task_progress_dependencies(task_1),
            TaskProgressDependencies::Known(HashSet::new()),
        );
        assert_eq!(
            registry.task_progress_dependencies(task_2),
            TaskProgressDependencies::Known([task_1].into_iter().collect()),
        );

        let err = registry
            .on_notified(task_2, LockTaken(lock_id))
            .unwrap_err();
        assert!(err.0.contains("already taken"), "returned error: {err}");
        assert_eq!(
            registry.task_progress_dependencies(task_1),
            TaskProgressDependencies::Known(HashSet::new()),
        );
        assert_eq!(
            registry.task_progress_dependencies(task_2),
            TaskProgressDependencies::Known([task_1].into_iter().collect()),
        );

        assert_eq!(
            registry.on_notified(task_1, LockReleased(lock_id)).unwrap(),
            NotificationOutcome::Acknowledged
        );
        assert_eq!(
            registry.task_progress_dependencies(task_1),
            TaskProgressDependencies::Known(HashSet::new()),
        );
        assert_eq!(
            registry.task_progress_dependencies(task_2),
            TaskProgressDependencies::Known(HashSet::new()),
        );

        assert_eq!(
            registry.on_notified(task_2, LockTaken(lock_id)).unwrap(),
            NotificationOutcome::Acknowledged
        );
        assert_eq!(
            registry.task_progress_dependencies(task_1),
            TaskProgressDependencies::Known(HashSet::new()),
        );
        assert_eq!(
            registry.task_progress_dependencies(task_2),
            TaskProgressDependencies::Known(HashSet::new()),
        );

        assert_eq!(
            registry.on_notified(task_2, LockReleased(lock_id)).unwrap(),
            NotificationOutcome::Acknowledged
        );
        assert_eq!(
            registry.task_progress_dependencies(task_1),
            TaskProgressDependencies::Known(HashSet::new()),
        );
        assert_eq!(
            registry.task_progress_dependencies(task_2),
            TaskProgressDependencies::Known(HashSet::new()),
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
            TaskProgressDependencies::Known(HashSet::new())
        );
        assert_eq!(
            registry.task_progress_dependencies(task_2),
            TaskProgressDependencies::Known(HashSet::new())
        );

        assert_eq!(
            registry.on_notified(task_1, TakingLock(lock_id)).unwrap(),
            NotificationOutcome::Acknowledged
        );
        assert_eq!(
            registry.task_progress_dependencies(task_1),
            TaskProgressDependencies::Known(HashSet::new())
        );
        assert_eq!(
            registry.task_progress_dependencies(task_2),
            TaskProgressDependencies::Known(HashSet::new())
        );
    }
}
