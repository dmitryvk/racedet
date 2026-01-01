use std::collections::HashMap;

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
}

impl std::fmt::Display for StringIdx {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}
