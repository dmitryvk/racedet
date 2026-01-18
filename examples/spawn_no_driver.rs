use std::{panic::AssertUnwindSafe, str::FromStr, time::Duration};

use futures::FutureExt;
use racedet::{
    ReplayTrace, new_scheduler,
    sync::task_wait::{NewTaskGroup, TaskGroup, TaskSpawned, TaskWaitAnyN, TaskWaitCompleted},
    task::{StartBarrier, Task, execution_point, sync_event, task},
    with_scheduler,
};
use tokio::{spawn, time::timeout};

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let replay = std::env::var("RACEDET_REPLAY")
        .ok()
        .map(|s| ReplayTrace::from_str(&s).unwrap());
    let (scheduler, control_fut) = new_scheduler(replay.as_ref());
    tokio::spawn(control_fut);
    let res = timeout(
        Duration::from_secs(10),
        with_scheduler(scheduler.clone(), AssertUnwindSafe(foo()).catch_unwind()),
    )
    .await;
    match res {
        Ok(Ok(res)) => {
            println!("ok {res:?}");
        }
        Ok(Err(panic)) => {
            println!(
                "test panicked: {message}",
                message = panic
                    .downcast_ref::<String>()
                    .map(String::as_str)
                    .unwrap_or("(non-String payload)")
            );
        }
        Err(_) => {
            println!("test timed out");
        }
    }
    println!(
        "To replay this execution, set this environment variable:\nRACEDET_REPLAY=\"{}\"",
        scheduler.get_replay()
    );
    println!("{}", scheduler.get_trace());
}

async fn foo() {
    task(Task::new("root"), root()).await;
}

async fn root() {
    execution_point("before spawn").await;

    // task_join means that the current task is waiting for nested tasks and should not be scheduled in of itself (but other tasks should be scheduled instead)
    let barrier = StartBarrier::new(2);
    let task_group = TaskGroup::new();
    sync_event(NewTaskGroup(task_group));
    let task_a = spawn(task(
        Task::new("child a")
            .with_task_group(task_group, 0)
            .with_start_barrier(barrier.clone()),
        child("a"),
    ));
    let task_b = spawn(task(
        Task::new("child b")
            .with_task_group(task_group, 1)
            .with_start_barrier(barrier.clone()),
        child("b"),
    ));
    sync_event(TaskSpawned(task_group, 0));
    sync_event(TaskSpawned(task_group, 1));
    tracing::debug!("sync_event starting join");
    sync_event(TaskWaitAnyN(task_group, 2));
    tracing::debug!("starting join");
    task_a.await.unwrap();
    task_b.await.unwrap();
    tracing::debug!("joined");
    sync_event(TaskWaitCompleted);
    tracing::info!("ok");
    execution_point("joined").await;
}

async fn child(name: &'static str) {
    execution_point("a1").await;
    let task_group = TaskGroup::new();
    sync_event(NewTaskGroup(task_group));
    let r = spawn(task(
        Task::new(format!("leaf {name}")).with_task_group(task_group, 0),
        execution_point("done"),
    ));
    sync_event(TaskSpawned(task_group, 0));
    sync_event(TaskWaitAnyN(task_group, 1));
    r.await.unwrap();
    sync_event(TaskWaitCompleted);
    execution_point("a2").await;
}
