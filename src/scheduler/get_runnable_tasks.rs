use std::collections::{BTreeSet, HashSet};

use crate::{
    sync::{TaskProgressDependencies, active::SyncModelRegistry},
    task::TaskId,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TaskScheduleChoice {
    pub(crate) chosen_task: TaskId,
    pub(crate) to_run: HashSet<TaskId>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum NextSchedulerAction {
    NoChoice(HashSet<TaskId>),
    Choices(Vec<TaskScheduleChoice>),
}

pub(crate) fn get_eligible_scheduler_choices(
    running_tasks: &HashSet<TaskId>,
    ready_tasks: &HashSet<TaskId>,
    sync: &SyncModelRegistry,
) -> NextSchedulerAction {
    let mut has_ready = false;
    let mut need_to_run = HashSet::new();
    for task_id in running_tasks {
        match task_transitive_deps(sync, *task_id) {
            TaskProgressDependencies::Ready {
                need_to_run: cur_transitive_need_to_run,
            } => {
                has_ready = true;
                need_to_run.extend(
                    cur_transitive_need_to_run
                        .into_iter()
                        .filter(|task_id| !running_tasks.contains(task_id)),
                );
            }
            TaskProgressDependencies::Blocked => {}
        }
    }

    if has_ready {
        return NextSchedulerAction::NoChoice(need_to_run);
    }

    let mut seen_task_sets = HashSet::<BTreeSet<TaskId>>::new();
    let mut choices = Vec::new();
    for &task_id in ready_tasks {
        match task_transitive_deps(sync, task_id) {
            TaskProgressDependencies::Ready { need_to_run } => {
                let deps: HashSet<TaskId> = need_to_run
                    .into_iter()
                    .filter(|task_id| !running_tasks.contains(task_id))
                    .collect();
                if seen_task_sets.insert(deps.iter().copied().collect()) {
                    choices.push(TaskScheduleChoice {
                        chosen_task: task_id,
                        to_run: deps,
                    });
                }
            }
            TaskProgressDependencies::Blocked => {}
        }
    }

    NextSchedulerAction::Choices(choices)
}

fn task_transitive_deps(sync: &SyncModelRegistry, task_id: TaskId) -> TaskProgressDependencies {
    match sync.task_progress_dependencies(task_id) {
        TaskProgressDependencies::Ready { .. } => {
            let mut visited = HashSet::new();
            let mut stack = Vec::new();
            stack.push(task_id);
            visited.insert(task_id);
            while let Some(cur_task_id) = stack.pop() {
                match sync.task_progress_dependencies(cur_task_id) {
                    TaskProgressDependencies::Ready { need_to_run } => {
                        for next_task in need_to_run {
                            if visited.insert(next_task) {
                                stack.push(next_task);
                            }
                        }
                    }
                    TaskProgressDependencies::Blocked => {}
                }
            }
            TaskProgressDependencies::Ready {
                need_to_run: visited,
            }
        }
        TaskProgressDependencies::Blocked => TaskProgressDependencies::Blocked,
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, num::NonZeroU64};

    use itertools::Itertools;

    use super::*;
    use crate::sync::{
        BadSync, DynSyncModel, NotificationOutcome, ProcessSyncEvent, SyncEvent, SyncModel,
    };

    #[test]
    fn ready_running() {
        let sync_registry = SyncModelRegistry::new();
        let task_ids = (1..=2)
            .map(|i| TaskId::new(NonZeroU64::new(i).unwrap()))
            .collect_vec();
        let choices = get_eligible_scheduler_choices(
            &[task_ids[0]].into_iter().collect(),
            &[task_ids[1]].into_iter().collect(),
            &sync_registry,
        );
        assert_eq!(choices, NextSchedulerAction::NoChoice(HashSet::new()));
    }

    #[test]
    fn deps_for_ready_running() {
        let mut sync_registry = SyncModelRegistry::new();
        let task_ids = (1..=4)
            .map(|i| TaskId::new(NonZeroU64::new(i).unwrap()))
            .collect_vec();
        sync_registry
            .on_notified(
                task_ids[0],
                ProvideDeps(TaskProgressDependencies::Ready {
                    need_to_run: [task_ids[1], task_ids[2]].into_iter().collect(),
                }),
            )
            .unwrap();
        sync_registry
            .on_notified(task_ids[1], ProvideDeps(TaskProgressDependencies::Blocked))
            .unwrap();
        let choices = get_eligible_scheduler_choices(
            &[task_ids[0]].into_iter().collect(),
            &[task_ids[1], task_ids[2], task_ids[3]]
                .into_iter()
                .collect(),
            &sync_registry,
        );
        assert_eq!(
            choices,
            NextSchedulerAction::NoChoice([task_ids[1], task_ids[2]].into_iter().collect())
        );
    }

    #[test]
    fn pick_new_ready_if_no_running() {
        let mut sync_registry = SyncModelRegistry::new();
        let task_ids = (1..=4)
            .map(|i| TaskId::new(NonZeroU64::new(i).unwrap()))
            .collect_vec();
        sync_registry
            .on_notified(
                task_ids[0],
                ProvideDeps(TaskProgressDependencies::Ready {
                    need_to_run: [task_ids[1], task_ids[2]].into_iter().collect(),
                }),
            )
            .unwrap();
        sync_registry
            .on_notified(task_ids[1], ProvideDeps(TaskProgressDependencies::Blocked))
            .unwrap();
        let choices = get_eligible_scheduler_choices(
            &[].into_iter().collect(),
            &[task_ids[0], task_ids[1], task_ids[2], task_ids[3]]
                .into_iter()
                .collect(),
            &sync_registry,
        );

        let NextSchedulerAction::Choices(mut choices) = choices else {
            panic!("unexpected action: {choices:?}");
        };

        choices.sort_by_cached_key(|item| {
            (
                item.chosen_task,
                item.to_run.iter().copied().collect::<BTreeSet<_>>(),
            )
        });
        println!("{choices:#?}");
        assert_eq!(choices.len(), 3);
        assert_eq!(
            choices[0],
            TaskScheduleChoice {
                chosen_task: task_ids[0],
                to_run: [task_ids[0], task_ids[1], task_ids[2]]
                    .into_iter()
                    .collect()
            }
        );
        assert_eq!(
            choices[1],
            TaskScheduleChoice {
                chosen_task: task_ids[2],
                to_run: [task_ids[2]].into_iter().collect()
            }
        );
        assert_eq!(
            choices[2],
            TaskScheduleChoice {
                chosen_task: task_ids[3],
                to_run: [task_ids[3]].into_iter().collect()
            }
        );
    }

    #[test]
    fn complex_cases() {
        let mut sync_registry = SyncModelRegistry::new();
        let task_ids = (1..=7)
            .map(|i| TaskId::new(NonZeroU64::new(i).unwrap()))
            .collect_vec();

        // normal tasks:
        // 0: ready
        sync_registry
            .on_notified(
                task_ids[0],
                ProvideDeps(TaskProgressDependencies::Ready {
                    need_to_run: HashSet::new(),
                }),
            )
            .unwrap();
        // tokio::join
        // 1: ready, wait for 2
        // 2: blocked
        sync_registry
            .on_notified(
                task_ids[1],
                ProvideDeps(TaskProgressDependencies::Ready {
                    need_to_run: [task_ids[2]].into_iter().collect(),
                }),
            )
            .unwrap();
        sync_registry
            .on_notified(task_ids[2], ProvideDeps(TaskProgressDependencies::Blocked))
            .unwrap();
        // wait on mutex
        // 3: blocked
        sync_registry
            .on_notified(task_ids[3], ProvideDeps(TaskProgressDependencies::Blocked))
            .unwrap();
        // wait on barrier
        // 4: ready, wait for 5, 6
        // 5: ready, wait for 4, 6
        // 6: ready, wait for 4, 5
        sync_registry
            .on_notified(
                task_ids[4],
                ProvideDeps(TaskProgressDependencies::Ready {
                    need_to_run: [task_ids[5], task_ids[6]].into_iter().collect(),
                }),
            )
            .unwrap();
        sync_registry
            .on_notified(
                task_ids[5],
                ProvideDeps(TaskProgressDependencies::Ready {
                    need_to_run: [task_ids[4], task_ids[6]].into_iter().collect(),
                }),
            )
            .unwrap();
        sync_registry
            .on_notified(
                task_ids[6],
                ProvideDeps(TaskProgressDependencies::Ready {
                    need_to_run: [task_ids[4], task_ids[5]].into_iter().collect(),
                }),
            )
            .unwrap();

        let choices = get_eligible_scheduler_choices(
            &HashSet::new(),
            &task_ids.iter().copied().collect(),
            &sync_registry,
        );
        let NextSchedulerAction::Choices(mut choices) = choices else {
            panic!("unexpected action: {choices:?}");
        };

        choices.sort_by_cached_key(|item| {
            (
                item.chosen_task,
                item.to_run.iter().copied().collect::<BTreeSet<_>>(),
            )
        });

        println!("{choices:#?}");
        assert_eq!(choices.len(), 3);
        assert_eq!(
            choices[0],
            TaskScheduleChoice {
                chosen_task: task_ids[0],
                to_run: [task_ids[0]].into_iter().collect()
            }
        );
        assert_eq!(
            choices[1],
            TaskScheduleChoice {
                chosen_task: task_ids[1],
                to_run: [task_ids[1], task_ids[2]].into_iter().collect()
            }
        );
        assert!([task_ids[4], task_ids[5], task_ids[6]].contains(&choices[2].chosen_task));
        assert_eq!(
            choices[2].to_run,
            [task_ids[4], task_ids[5], task_ids[6]]
                .into_iter()
                .collect()
        );
    }

    #[derive(Debug, Default)]
    struct TestSyncModel {
        task_deps: HashMap<TaskId, TaskProgressDependencies>,
    }

    impl SyncModel for TestSyncModel {}
    impl DynSyncModel for TestSyncModel {
        fn task_progress_dependencies(&self, task_id: TaskId) -> TaskProgressDependencies {
            self.task_deps.get(&task_id).cloned().unwrap_or_else(|| {
                TaskProgressDependencies::Ready {
                    need_to_run: HashSet::new(),
                }
            })
        }
    }

    struct ProvideDeps(TaskProgressDependencies);
    impl SyncEvent for ProvideDeps {
        type Model = TestSyncModel;
    }
    impl ProcessSyncEvent<ProvideDeps> for TestSyncModel {
        fn on_event(
            &mut self,
            task_id: TaskId,
            ProvideDeps(deps): ProvideDeps,
        ) -> Result<NotificationOutcome, BadSync> {
            self.task_deps.insert(task_id, deps);
            Ok(NotificationOutcome::Acknowledged)
        }
    }
}
