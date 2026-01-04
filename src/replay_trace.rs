use std::{
    collections::{HashMap, HashSet},
    str::FromStr,
};

use itertools::Itertools;

use crate::{
    TaskStableId,
    full_trace::{Trace, TraceItem},
};

#[derive(Debug, Clone)]
pub struct ReplayTrace {
    strings: StringPool,
    steps: Vec<ReplayStep>,
}

#[derive(Debug, Clone)]
struct StringPool {
    vec: Vec<String>,
    map: HashMap<String, StringIdx>,
}

#[derive(Debug, Clone, Copy)]
struct StringIdx(usize);

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
    pub(crate) fn from_trace(trace: &Trace) -> Self {
        let mut strings = StringPool::new();
        let steps = trace
            .trace
            .iter()
            .filter_map(|item| Self::step_from_item(item, &mut strings))
            .collect();
        Self { strings, steps }
    }

    fn step_from_item(item: &TraceItem, strings: &mut StringPool) -> Option<ReplayStep> {
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
        suspended_tasks: &HashSet<(TaskStableId, &str, &str)>,
    ) -> Result<&[TaskStableId], String> {
        tracing::debug!("strings={:?}", self.strings);
        let step = self
            .steps
            .get(step_idx)
            .ok_or_else(|| "no more steps".to_owned())?;
        tracing::debug!("step={step:?}");
        if step.suspended_tasks.len() != suspended_tasks.len()
            || !step.suspended_tasks.iter().all(|task| {
                let name = self.strings.get(task.name).unwrap();
                let pos = self.strings.get(task.position).unwrap();
                if suspended_tasks.contains(&(task.id, name, pos)) {
                    true
                } else {
                    tracing::error!(
                        "replay diverged: suspended_tasks={suspended_tasks:?} doesn't contain {:?}",
                        (task.id, name, pos)
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
                        name = task.name,
                        position = task.position,
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
        for string in &self.strings.vec {
            if written {
                write!(f, ",")?;
            }
            // TODO: escaping
            write!(f, "{string}")?;
            written = true;
        }
        Ok(())
    }
}

impl StringPool {
    fn new() -> Self {
        Self {
            vec: Vec::new(),
            map: HashMap::new(),
        }
    }

    fn intern(&mut self, value: &str) -> StringIdx {
        if let Some(idx) = self.map.get(value) {
            return *idx;
        };
        let idx = StringIdx(self.vec.len() + 1);
        self.vec.push(value.to_owned());
        self.map.insert(value.to_owned(), idx);
        idx
    }

    fn get(&self, idx: StringIdx) -> Option<&str> {
        self.vec.get(idx.0 - 1).map(|s| s.as_str())
    }
}

impl std::fmt::Display for StringIdx {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
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

impl FromStr for ReplayTrace {
    type Err = ParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (s_steps, s_strings) = s.split_once('#').ok_or(ParseError("no #"))?;
        tracing::debug!("s_strings={s_strings}");

        let mut strings = StringPool::new();
        for string in s_strings.split(',') {
            _ = strings.intern(string);
        }

        let mut steps = Vec::new();
        for step in s_steps.split(';') {
            steps.push(step.parse()?);
        }

        Ok(Self { strings, steps })
    }
}

impl FromStr for ReplayStep {
    type Err = ParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (s_suspended, s_resumed) = s.split_once('/').ok_or(ParseError("no /"))?;
        let mut suspended = Vec::new();
        let mut resumed = Vec::new();
        for task in s_suspended.split(',') {
            let [s_id, s_name, s_position] = task
                .split('-')
                .collect_array()
                .ok_or(ParseError("wrong number of -"))?;
            suspended.push(SuspendedTask {
                id: TaskStableId(s_id.parse().map_err(|_| ParseError("parse task id"))?),
                name: StringIdx(s_name.parse().map_err(|_| ParseError("parse task name"))?),
                position: StringIdx(
                    s_position
                        .parse()
                        .map_err(|_| ParseError("parse task position"))?,
                ),
            })
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
