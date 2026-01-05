use std::str::FromStr;

use itertools::Itertools;

use crate::TaskStableId;

#[derive(Debug, Clone)]
pub struct ReplayTrace {
    pub(crate) strings: Vec<String>,
    pub(crate) steps: Vec<ReplayStep>,
}

#[derive(Debug, Clone)]
pub(crate) struct ReplayStep {
    pub(crate) suspended_tasks: Vec<SuspendedTask>,
    pub(crate) resumed_tasks: Vec<TaskStableId>,
}

#[derive(Debug, Clone)]
pub(crate) struct SuspendedTask {
    pub(crate) id: TaskStableId,
    pub(crate) name: u32,
    pub(crate) position: u32,
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
        for s in &self.strings {
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

impl FromStr for ReplayTrace {
    type Err = ParseError;

    fn from_str(s: &str) -> Result<Self, ParseError> {
        let (s_steps, s_strings) = s.split_once('#').ok_or(ParseError("no #"))?;
        tracing::debug!("s_strings={s_strings}");

        let strings: Vec<String> = s_strings.split(',').map(|s| s.to_owned()).collect();

        let mut steps = Vec::new();
        for step in s_steps.split(';') {
            steps.push(ReplayStep::from_str(step)?);
        }

        Ok(Self { strings, steps })
    }
}

impl FromStr for ReplayStep {
    type Err = ParseError;
    fn from_str(s: &str) -> Result<Self, ParseError> {
        let (s_suspended, s_resumed) = s.split_once('/').ok_or(ParseError("no /"))?;
        let mut suspended = Vec::new();
        let mut resumed = Vec::new();
        for task in s_suspended.split(',') {
            let [s_id, s_name, s_position] = task
                .split('-')
                .collect_array()
                .ok_or(ParseError("wrong number of -"))?;
            let id = TaskStableId(s_id.parse().map_err(|_| ParseError("parse task id"))?);
            let name = u32::from_str(s_name).map_err(|_| ParseError("parse task name"))?;
            let position =
                u32::from_str(s_position).map_err(|_| ParseError("parse task position"))?;
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
