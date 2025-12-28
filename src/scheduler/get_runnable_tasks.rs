use std::collections::{BTreeSet, HashSet};

use crate::{
    TaskId,
    sync_model::{SyncModelRegistry, TaskProgressDependencies},
};

pub(crate) struct TasksToRun {
    chosen_task: Option<TaskId>,
    to_run: HashSet<TaskId>,
}

pub(crate) fn get_runnable_tasks(
    running_tasks: HashSet<TaskId>,
    suspended_tasks: HashSet<TaskId>,
    sync: &SyncModelRegistry,
) -> Vec<TasksToRun> {
    let mut has_ready = false;
    let mut need_to_run = HashSet::new();
    for task_id in &running_tasks {
        let deps = sync.task_progress_dependencies(*task_id);
        match deps {
            TaskProgressDependencies::Ready { .. } => {
                has_ready = true;
                need_to_run.extend(
                    task_transitive_deps(sync, *task_id)
                        .into_iter()
                        .filter(|task_id| !running_tasks.contains(task_id)),
                );
            }
            TaskProgressDependencies::Blocked => {}
        }
    }

    if has_ready {
        return vec![TasksToRun {
            chosen_task: None,
            to_run: need_to_run,
        }];
    }

    let mut seen_task_sets = HashSet::<BTreeSet<TaskId>>::new();
    let mut result = Vec::new();
    for &task_id in &suspended_tasks {
        match sync.task_progress_dependencies(task_id) {
            TaskProgressDependencies::Ready { .. } => {
                let deps: HashSet<TaskId> = task_transitive_deps(sync, task_id)
                    .into_iter()
                    .filter(|task_id| !running_tasks.contains(&task_id))
                    .collect();
                if seen_task_sets.insert(deps.iter().copied().collect()) {
                    result.push(TasksToRun {
                        chosen_task: Some(task_id),
                        to_run: deps,
                    })
                }
            }
            TaskProgressDependencies::Blocked => {}
        }
    }

    result
}

fn task_transitive_deps(sync: &SyncModelRegistry, task_id: TaskId) -> HashSet<TaskId> {
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

    visited
}
