use itertools::Itertools;

use crate::{
    TaskStableId,
    full_trace::{Trace, TraceItem},
};

#[derive(Debug, Clone)]
pub struct ReplayTrace {
    steps: Vec<ReplayStep>,
}

#[derive(Debug, Clone)]
pub struct ReplayStep {
    suspended_tasks: Vec<SuspendedTask>,
    resumed_tasks: Vec<TaskRef>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct TaskRef {
    pub id: TaskStableId,
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct SuspendedTask {
    pub id: TaskStableId,
    pub name: String,
    pub position: String,
}

impl ReplayTrace {
    pub(crate) fn from_trace(trace: &Trace) -> Self {
        Self {
            steps: trace
                .trace
                .iter()
                .filter_map(Self::step_from_item)
                .collect(),
        }
    }

    fn step_from_item(item: &TraceItem) -> Option<ReplayStep> {
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
                    name: task.name.clone(),
                    position: task
                        .position
                        .clone()
                        .expect("suspended tasks have position"),
                })
                .collect(),
            resumed_tasks: resumed_tasks
                .iter()
                .map(|task| TaskRef {
                    id: task.id.stable_id().expect("resumed tasks have stable id"),
                    name: task.name.clone(),
                })
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
                    .map(|task| format!("{id}-{name}", id = task.id, name = task.name,))
                    .join(","),
            )?;
            written = true;
        }
        Ok(())
    }
}
