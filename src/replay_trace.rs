use std::{collections::HashSet, str::FromStr, sync::Arc};

use itertools::Itertools;

use crate::{
    TaskStableId,
    full_trace::{Trace, TraceItem},
    string_pool::{StringIdx, StringPool},
};

#[derive(Debug, Clone)]
pub struct ReplayTrace {
    strings: Arc<StringPool>,
    steps: Vec<ReplayStep>,
}

#[derive(Debug, Clone)]
pub struct ReplayStep {
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
            || !step.suspended_tasks.iter().all(|task| {
                if suspended_tasks.contains(&(task.id, task.name, task.position)) {
                    true
                } else {
                    tracing::error!(
                        "replay diverged: suspended_tasks={suspended_tasks:?} doesn't contain {:?}",
                        (
                            task.id,
                            self.strings
                                .get(task.name)
                                .expect("string pool contains all strings"),
                            self.strings
                                .get(task.position)
                                .expect("string pool has all strings")
                        )
                    );
                    false
                }
            })
        {
            Err(format!(
                "replay diverged: suspended tasks don't match: expected {:?}, got {suspended_tasks:?}",
                step.suspended_tasks
                    .iter()
                    .map(|task| (
                        task.id,
                        self.strings.get(task.name).unwrap(),
                        self.strings.get(task.position).unwrap()
                    ))
                    .collect_vec()
            ))
        } else {
            Ok(&step.resumed_tasks)
        }
    }
}

impl std::fmt::Display for ReplayTrace {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let new_string_pool = StringPool::new();
        let mut written = false;
        for step in &self.steps {
            if written {
                write!(f, ";")?;
            }
            write!(
                f,
                "{suspended}/{resumed}",
                suspended = step
                    .suspended_tasks
                    .iter()
                    .map(|task| format!(
                        "{id}-{name}-{position}",
                        id = task.id,
                        name = new_string_pool.intern(
                            &self
                                .strings
                                .get(task.name)
                                .expect("string pool has all strings")
                        ),
                        position = new_string_pool.intern(
                            &self
                                .strings
                                .get(task.position)
                                .expect("string pool has all strings")
                        ),
                    ))
                    .join(","),
                resumed = step
                    .resumed_tasks
                    .iter()
                    .map(|id| format!("{id}"))
                    .join(","),
            )?;
            written = true;
        }
        write!(f, "#")?;
        let mut written = false;
        for (_, s) in new_string_pool.iter_ordered() {
            if written {
                write!(f, ",")?;
            }
            // TODO: escaping
            write!(f, "{s}")?;
            written = true;
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct ParseError(&'static str);

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for ParseError {}

impl ReplayTrace {
    pub(crate) fn from_str(strings: Arc<StringPool>, s: &str) -> Result<Self, ParseError> {
        let (s_steps, s_strings) = s.split_once('#').ok_or(ParseError("no #"))?;
        tracing::debug!("s_strings={s_strings}");

        let string_ids: Vec<StringIdx> = s_strings.split(',').map(|s| strings.intern(s)).collect();

        let mut steps = Vec::new();
        for step in s_steps.split(';') {
            steps.push(ReplayStep::from_str(&string_ids, step)?);
        }

        Ok(Self { strings, steps })
    }
}

impl ReplayStep {
    fn from_str(strings: &[StringIdx], s: &str) -> Result<Self, ParseError> {
        let (s_suspended, s_resumed) = s.split_once('/').ok_or(ParseError("no /"))?;
        let mut suspended = Vec::new();
        let mut resumed = Vec::new();
        for task in s_suspended.split(',') {
            let [s_id, s_name, s_position] = task
                .split('-')
                .collect_array()
                .ok_or(ParseError("wrong number of -"))?;
            let id = TaskStableId(s_id.parse().map_err(|_| ParseError("parse task id"))?);
            let name =
                strings[usize::from_str(s_name).map_err(|_| ParseError("parse task name"))? - 1];
            let position = strings
                [usize::from_str(s_position).map_err(|_| ParseError("parse task position"))? - 1];
            suspended.push(SuspendedTask { id, name, position })
        }
        for task in s_resumed.split(',') {
            let task_id = TaskStableId(task.parse().map_err(|_| ParseError("parse task id"))?);
            resumed.push(task_id);
        }

        Ok(Self {
            suspended_tasks: suspended,
            resumed_tasks: resumed,
        })
    }
}
