use std::{collections::HashSet, sync::Arc};

use crate::{
    TaskStableId,
    full_trace::{Trace, TraceItem},
    string_pool::{StringIdx, StringPool},
};

#[derive(Debug, Clone)]
pub(crate) struct ReplayTrace {
    strings: Arc<StringPool>,
    steps: Vec<ReplayStep>,
}

#[derive(Debug, Clone)]
struct ReplayStep {
    suspended_tasks: Vec<SuspendedTask>,
    resumed_tasks: Vec<TaskStableId>,
}

#[derive(Debug, Clone)]
struct SuspendedTask {
    id: TaskStableId,
    name: StringIdx,
    position: StringIdx,
}

impl ReplayTrace {
    pub(crate) fn from_parsed(
        string_pool: Arc<StringPool>,
        trace: &crate::replay_trace_parsed::ReplayTrace,
    ) -> Self {
        let string_ids: Vec<StringIdx> = trace
            .strings
            .iter()
            .map(|s| string_pool.intern(s))
            .collect();
        let steps = trace
            .steps
            .iter()
            .map(|s| ReplayStep {
                suspended_tasks: s
                    .suspended_tasks
                    .iter()
                    .map(|t| SuspendedTask {
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

    pub(crate) fn to_parsed(&self) -> crate::replay_trace_parsed::ReplayTrace {
        let new_string_pool = StringPool::new();
        let steps = self
            .steps
            .iter()
            .map(|s| crate::replay_trace_parsed::ReplayStep {
                suspended_tasks: s
                    .suspended_tasks
                    .iter()
                    .map(|t| crate::replay_trace_parsed::SuspendedTask {
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
        crate::replay_trace_parsed::ReplayTrace { strings, steps }
    }

    pub(crate) fn from_trace(string_pool: Arc<StringPool>, trace: &Trace) -> Self {
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

    fn step_from_item(item: &TraceItem, strings: &StringPool) -> Option<ReplayStep> {
        let TraceItem::ScheduleDecision {
            resumed_tasks,
            running_tasks: _,
            suspended_tasks,
            options: _,
        } = item
        else {
            return None;
        };
        let step = ReplayStep {
            suspended_tasks: suspended_tasks
                .iter()
                .map(|task| SuspendedTask {
                    id: task.id.stable_id().expect("suspended tasks have stable id"),
                    name: strings.intern(&task.name),
                    position: strings.intern(
                        task.position
                            .as_ref()
                            .expect("suspended tasks have position"),
                    ),
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
        suspended_tasks: &HashSet<(TaskStableId, StringIdx, StringIdx)>,
    ) -> Result<&[TaskStableId], String> {
        let step = self
            .steps
            .get(step_idx)
            .ok_or_else(|| "no more steps".to_owned())?;
        tracing::debug!("step={step:?}");
        if step.suspended_tasks.len() != suspended_tasks.len()
            || !step
                .suspended_tasks
                .iter()
                .all(|task| suspended_tasks.contains(&(task.id, task.name, task.position)))
        {
            let mut expected = step
                .suspended_tasks
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
            let mut actual = suspended_tasks
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
                "replay diverged: suspended tasks don't match: expected {expected:?}, got {actual:?}"
            ))
        } else {
            Ok(&step.resumed_tasks)
        }
    }
}
