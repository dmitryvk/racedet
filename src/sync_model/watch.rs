use std::collections::{HashMap, HashSet};

use crate::{
    TaskId,
    sync_model::{
        BadSyncError, DynSyncModel, NotificationOutcome, ProcessSyncEvent, SyncEvent, SyncModel,
        TaskProgressDependencies,
    },
};

#[derive(Default)]
pub struct WatchModel {
    tasks: HashMap<TaskId, WatchId>,
    waiters: HashMap<WatchId, HashSet<TaskId>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct WatchId(usize);

impl WatchId {
    pub fn from_sender<T>(sender: &tokio::sync::watch::Sender<T>) -> Self {
        let borrow = sender.borrow();
        let addr = &*borrow as *const T as usize;
        Self(addr)
    }

    pub fn from_receiver<T>(receiver: &tokio::sync::watch::Receiver<T>) -> Self {
        let borrow = receiver.borrow();
        let addr = &*borrow as *const T as usize;
        Self(addr)
    }
}

pub struct WaitingForWatchUpdate(pub WatchId);
impl SyncEvent for WaitingForWatchUpdate {
    type Model = WatchModel;
}
pub struct WatchNotified(pub WatchId);
impl SyncEvent for WatchNotified {
    type Model = WatchModel;
}

impl DynSyncModel for WatchModel {
    fn task_progress_dependencies(&self, task_id: TaskId) -> TaskProgressDependencies {
        if self.tasks.contains_key(&task_id) {
            TaskProgressDependencies::Blocked
        } else {
            TaskProgressDependencies::Ready {
                need_to_run: HashSet::new(),
            }
        }
    }
}
impl SyncModel for WatchModel {}

impl ProcessSyncEvent<WaitingForWatchUpdate> for WatchModel {
    fn on_event(
        &mut self,
        task_id: TaskId,
        WaitingForWatchUpdate(watch_id): WaitingForWatchUpdate,
    ) -> Result<NotificationOutcome, BadSyncError> {
        self.tasks.insert(task_id, watch_id);
        self.waiters.entry(watch_id).or_default().insert(task_id);
        Ok(NotificationOutcome::ScheduleRequired)
    }
}

impl ProcessSyncEvent<WatchNotified> for WatchModel {
    fn on_event(
        &mut self,
        _task_id: TaskId,
        WatchNotified(watch_id): WatchNotified,
    ) -> Result<NotificationOutcome, BadSyncError> {
        if let Some(waiters) = self.waiters.remove(&watch_id) {
            for task_id in waiters {
                self.tasks.remove(&task_id);
            }
        }
        Ok(NotificationOutcome::Acknowledged)
    }
}
