use std::collections::BTreeSet;

use itertools::Itertools;

use crate::TaskId;

#[derive(Debug, Clone)]
pub struct Trace {
    pub trace: Vec<TraceItem>,
}

impl std::fmt::Display for Trace {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for (i, step) in self.trace.iter().enumerate() {
            writeln!(f, "{i}. {step}")?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub enum TraceItem {
    TaskStarted(TaskRef),
    TaskFinished(TaskRef),
    TaskSuspended {
        task: TaskRef,
        suspend_point: String,
    },
    AutoResumedTasks {
        resumed_tasks: BTreeSet<TaskRef>,
        running_tasks: BTreeSet<TaskRef>,
        suspended_tasks: BTreeSet<TaskRef>,
    },
    ScheduleDecision {
        resumed_tasks: BTreeSet<TaskRef>,
        running_tasks: BTreeSet<TaskRef>,
        suspended_tasks: BTreeSet<TaskRef>,
        options: Vec<BTreeSet<TaskRef>>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct TaskRef {
    pub id: TaskId,
    pub name: String,
}

impl std::fmt::Display for TraceItem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TraceItem::TaskStarted(task_ref) => write!(f, "start {task_ref}"),
            TraceItem::TaskFinished(task_ref) => write!(f, "finish {task_ref}"),
            TraceItem::TaskSuspended {
                task,
                suspend_point,
            } => write!(f, "suspend {task} {suspend_point}"),
            TraceItem::AutoResumedTasks {
                resumed_tasks,
                running_tasks,
                suspended_tasks,
            } => write!(
                f,
                "auto-resumed [{resumed_tasks}] (run: [{running_tasks}], suspended: [{suspended_tasks}])",
                resumed_tasks = resumed_tasks.iter().map(ToString::to_string).join(", "),
                running_tasks = running_tasks.iter().map(ToString::to_string).join(", "),
                suspended_tasks = suspended_tasks.iter().map(ToString::to_string).join(", "),
            ),
            TraceItem::ScheduleDecision {
                resumed_tasks,
                running_tasks,
                suspended_tasks,
                options,
            } => write!(
                f,
                "resumed [{resumed_tasks}] (run: [{running_tasks}], suspended: [{suspended_tasks}], options: [{options}])",
                resumed_tasks = resumed_tasks.iter().map(ToString::to_string).join(", "),
                running_tasks = running_tasks.iter().map(ToString::to_string).join(", "),
                suspended_tasks = suspended_tasks.iter().map(ToString::to_string).join(", "),
                options = options
                    .iter()
                    .map(|option| format!(
                        "[{}]",
                        option.iter().map(ToString::to_string).join(", ")
                    ))
                    .join(", "),
            ),
        }
    }
}

impl std::fmt::Display for TaskRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {}", self.id, self.name)
    }
}
