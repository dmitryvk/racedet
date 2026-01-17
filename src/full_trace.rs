use std::{collections::BTreeSet, sync::Arc};

use itertools::Itertools;

use crate::{TaskId, TaskStableId};

#[derive(Debug, Clone)]
pub struct TraceView {
    pub(crate) trace: Vec<TraceViewItem>,
}

impl std::fmt::Display for TraceView {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for (i, step) in self.trace.iter().enumerate() {
            writeln!(f, "{i}. {step}")?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub(crate) enum TraceViewItem {
    TaskStarted(TaskRef),
    TaskFinished(TaskRef),
    TaskSuspended {
        task: TaskRef,
        suspend_point: Arc<str>,
    },
    AutoResumedTasks {
        resumed_tasks: BTreeSet<TaskRef>,
        running_tasks: BTreeSet<TaskSnapshot>,
        suspended_tasks: BTreeSet<TaskSnapshot>,
    },
    ScheduleDecision {
        resumed_tasks: BTreeSet<TaskRef>,
        running_tasks: BTreeSet<TaskSnapshot>,
        suspended_tasks: BTreeSet<TaskSnapshot>,
        options: Vec<BTreeSet<TaskRef>>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum FullTraceTaskId {
    Stable(TaskStableId),
    Unstable(TaskId),
}

impl FullTraceTaskId {
    pub(crate) fn stable_id(&self) -> Option<TaskStableId> {
        match self {
            FullTraceTaskId::Stable(id) => Some(*id),
            FullTraceTaskId::Unstable(_) => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct TaskRef {
    pub(crate) id: FullTraceTaskId,
    pub(crate) name: Arc<str>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct TaskSnapshot {
    pub(crate) id: FullTraceTaskId,
    pub(crate) name: Arc<str>,
    pub(crate) position: Option<Arc<str>>,
}

impl std::fmt::Display for TraceViewItem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TraceViewItem::TaskStarted(task_ref) => write!(f, "start {task_ref}"),
            TraceViewItem::TaskFinished(task_ref) => write!(f, "finish {task_ref}"),
            TraceViewItem::TaskSuspended {
                task,
                suspend_point,
            } => write!(f, "suspend {task} {suspend_point}"),
            TraceViewItem::AutoResumedTasks {
                resumed_tasks,
                running_tasks,
                suspended_tasks,
            } => write!(
                f,
                "auto-resumed [{resumed_tasks}] (run: [{running_tasks}], suspended: \
                 [{suspended_tasks}])",
                resumed_tasks = resumed_tasks.iter().map(ToString::to_string).join(", "),
                running_tasks = running_tasks.iter().map(ToString::to_string).join(", "),
                suspended_tasks = suspended_tasks.iter().map(ToString::to_string).join(", "),
            ),
            TraceViewItem::ScheduleDecision {
                resumed_tasks,
                running_tasks,
                suspended_tasks,
                options,
            } => write!(
                f,
                "resumed [{resumed_tasks}] (run: [{running_tasks}], suspended: \
                 [{suspended_tasks}], options: [{options}])",
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

impl std::fmt::Display for FullTraceTaskId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FullTraceTaskId::Stable(task_stable_id) => write!(f, "{task_stable_id}"),
            FullTraceTaskId::Unstable(task_id) => write!(f, "#{task_id}"),
        }
    }
}

impl std::fmt::Display for TaskRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{id} {name}", id = self.id, name = self.name)
    }
}

impl std::fmt::Display for TaskSnapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{id} {name} at {position}",
            id = self.id,
            name = self.name,
            position = self.position.as_deref().unwrap_or("(spawned)")
        )
    }
}
