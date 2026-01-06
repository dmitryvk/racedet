use std::{str::FromStr, time::Duration};

use conc_checker::{
    ReplayTrace,
    capture_panics::capture_panic,
    current_scheduler, execution_point, maybe_with_scheduler, new_scheduler, new_start_barrier,
    sync_event,
    sync_model::task_wait::{NewTaskGroup, TaskGroup, TaskWaitAnyN, TaskWaitCompleted},
    task, with_scheduler, with_start_barrier, with_task_group,
};
use timeout_tracing::{CaptureSpanAndStackTrace, timeout};
use tokio::{spawn, time::sleep};

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();
    let replay = std::env::var("CONC_CHECKER_REPLAY")
        .ok()
        .map(|s| ReplayTrace::from_str(&s).unwrap());
    let (scheduler, control_fut) = new_scheduler(replay.as_ref());
    tokio::spawn(control_fut);
    let res = timeout(
        Duration::from_secs(10),
        CaptureSpanAndStackTrace,
        with_scheduler(scheduler.clone(), capture_panic(foo())),
    )
    .await;
    match res {
        Ok(Ok(res)) => {
            println!("ok {res:?}");
        }
        Ok(Err(panic)) => {
            println!(
                "panic {} at {}\n{}",
                panic.message, panic.location, panic.backtrace
            );
        }
        Err(timeout) => {
            println!("timeout {}", timeout.active_traces[0].stack_trace());
        }
    }
    println!("{}", scheduler.get_trace());
    println!("CONC_CHECKER_REPLAY=\"{}\"", scheduler.get_replay());
}

async fn foo() {
    task("bar", bar()).await;
}

async fn bar() {
    execution_point("before spawn").await;

    // task_join means that the current task is waiting for nested tasks and should not be scheduled in of itself (but other tasks should be scheduled instead)
    let barrier = new_start_barrier(2);
    let task_group = TaskGroup::new();
    sync_event(NewTaskGroup(task_group));
    let task_a = spawn(maybe_with_scheduler(
        current_scheduler(),
        task(
            "spawn a",
            with_task_group(task_group, 0, with_start_barrier(barrier.clone(), baz(1))),
        ),
    ));
    let task_b = spawn(maybe_with_scheduler(
        current_scheduler(),
        task(
            "spawn b",
            with_task_group(task_group, 1, with_start_barrier(barrier.clone(), baz(2))),
        ),
    ));
    // TODO: sleep is a hack to ensure that spawn happens before the task becomes blocked
    sleep(Duration::from_millis(1)).await;
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

async fn baz(n: u32) {
    execution_point("a1").await;
    let task_group = TaskGroup::new();
    sync_event(NewTaskGroup(task_group));
    let r = spawn(maybe_with_scheduler(
        current_scheduler(),
        task(
            format!("spawn baz {n}"),
            with_task_group(task_group, 0, execution_point("q")),
        ),
    ));
    // TODO: call TaskWaitAnyN before spawn
    // TODO: TaskWaitAnyN introduces non-determinism if a child task is spawned when the current task is "blocked"
    //       this can be solved with "spawn promises"
    // TODO: sleep is a hack to ensure that spawn happens before the task becomes blocked
    sleep(Duration::from_millis(1)).await;
    sync_event(TaskWaitAnyN(task_group, 1));
    r.await.unwrap();
    sync_event(TaskWaitCompleted);
    execution_point("a2").await;
}
