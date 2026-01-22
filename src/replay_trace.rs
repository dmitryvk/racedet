use std::{collections::HashSet, sync::Arc};

use crate::{
    parsed_replay_trace::ReplayTrace,
    scheduler::active::TaskStableId,
    string_pool::{StringIdx, StringPool},
    trace::{TraceView, TraceViewItem},
};

#[derive(Debug, Clone)]
pub(crate) struct AttachedReplayTrace {
    strings: Arc<StringPool>,
    steps: Vec<ReplayStep>,
}

#[derive(Debug, Clone)]
struct ReplayStep {
    ready_tasks: Vec<ReadyTask>,
    resumed_tasks: Vec<TaskStableId>,
}

#[derive(Debug, Clone)]
struct ReadyTask {
    id: TaskStableId,
    name: StringIdx,
    position: StringIdx,
}

impl AttachedReplayTrace {
    pub(crate) fn from_parsed(string_pool: Arc<StringPool>, trace: &ReplayTrace) -> Self {
        let string_ids: Vec<StringIdx> = trace
            .strings
            .iter()
            .map(|s| string_pool.intern(s))
            .collect();
        let steps = trace
            .steps
            .iter()
            .map(|s| ReplayStep {
                ready_tasks: s
                    .ready_tasks
                    .iter()
                    .map(|t| ReadyTask {
                        id: t.id,
                        name: string_ids[t.name as usize - 1],
                        position: string_ids[t.position as usize - 1],
                    })
                    .collect(),
                resumed_tasks: s.resumed_tasks.clone(),
            })
            .collect();
        Self {
            strings: string_pool,
            steps,
        }
    }

    pub(crate) fn to_parsed(&self) -> crate::parsed_replay_trace::ReplayTrace {
        let new_string_pool = StringPool::new();
        let steps = self
            .steps
            .iter()
            .map(|s| crate::parsed_replay_trace::ReplayStep {
                ready_tasks: s
                    .ready_tasks
                    .iter()
                    .map(|t| crate::parsed_replay_trace::ReadyTask {
                        id: t.id,
                        name: new_string_pool
                            .intern(
                                &self
                                    .strings
                                    .get(t.name)
                                    .expect("string pool has all strings"),
                            )
                            .as_usize() as u32,
                        position: new_string_pool
                            .intern(
                                &self
                                    .strings
                                    .get(t.position)
                                    .expect("string pool has all strings"),
                            )
                            .as_usize() as u32,
                    })
                    .collect(),
                resumed_tasks: s.resumed_tasks.clone(),
            })
            .collect::<Vec<_>>();
        let strings = new_string_pool
            .iter_ordered()
            .map(|(_, s)| s.as_ref().to_owned())
            .collect::<Vec<_>>();
        crate::parsed_replay_trace::ReplayTrace { strings, steps }
    }

    pub(crate) fn from_trace(string_pool: Arc<StringPool>, trace: &TraceView) -> Self {
        let steps = trace
            .trace
            .iter()
            .filter_map(|item| Self::step_from_item(item, &string_pool))
            .collect();
        Self {
            strings: string_pool,
            steps,
        }
    }

    fn step_from_item(item: &TraceViewItem, strings: &StringPool) -> Option<ReplayStep> {
        let TraceViewItem::ScheduleDecision {
            resumed_tasks,
            running_tasks: _,
            ready_tasks,
            options: _,
        } = item
        else {
            return None;
        };
        let step = ReplayStep {
            ready_tasks: ready_tasks
                .iter()
                .map(|task| ReadyTask {
                    id: task.id.stable_id().expect("ready tasks have stable id"),
                    name: strings.intern(&task.name),
                    position: strings
                        .intern(task.position.as_ref().expect("ready tasks have position")),
                })
                .collect(),
            resumed_tasks: resumed_tasks
                .iter()
                .map(|task| task.id.stable_id().expect("resumed tasks have stable id"))
                .collect(),
        };
        Some(step)
    }

    pub(crate) fn get_resumed_tasks(
        &self,
        step_idx: usize,
        ready_tasks: &HashSet<(TaskStableId, StringIdx, StringIdx)>,
    ) -> Result<&[TaskStableId], String> {
        let step = self
            .steps
            .get(step_idx)
            .ok_or_else(|| "no more steps".to_owned())?;
        tracing::debug!("step={step:?}");
        if step.ready_tasks.len() != ready_tasks.len()
            || !step
                .ready_tasks
                .iter()
                .all(|task| ready_tasks.contains(&(task.id, task.name, task.position)))
        {
            let mut expected = step
                .ready_tasks
                .iter()
                .map(|task| {
                    (
                        task.id,
                        self.strings
                            .get(task.name)
                            .expect("string pool contains all strings"),
                        self.strings
                            .get(task.position)
                            .expect("string pool has all strings"),
                    )
                })
                .collect::<Vec<_>>();
            let mut actual = ready_tasks
                .iter()
                .map(|(id, name, position)| {
                    (
                        id,
                        self.strings
                            .get(*name)
                            .expect("string pool contains all strings"),
                        self.strings
                            .get(*position)
                            .expect("string pool has all strings"),
                    )
                })
                .collect::<Vec<_>>();
            expected.sort();
            actual.sort();
            tracing::error!("replay diverged: expected {expected:?}, got {actual:?}");
            Err(format!(
                "replay diverged: ready tasks don't match: expected {expected:?}, got \
                 {actual:?}"
            ))
        } else {
            Ok(&step.resumed_tasks)
        }
    }
}
