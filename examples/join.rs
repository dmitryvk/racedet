use std::str::FromStr;
use std::time::Duration;

use conc_checker::capture_panics::capture_panic;
use conc_checker::sync_model::task_wait::{
    NewTaskGroup, TaskGroup, TaskWaitAnyN, TaskWaitCompleted,
};
use conc_checker::{
    ReplayTrace, execution_point, new_start_barrier, task, with_start_barrier, with_task_group,
};
use conc_checker::{new_scheduler, sync_event, with_scheduler};
use futures::FutureExt;
use futures::select;
use timeout_tracing::{CaptureSpanAndStackTrace, timeout};
use tokio::{join, time::sleep};

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
    execution_point("spawn tasks").await;

    // task_join means that the current task is waiting for nested tasks and should not be scheduled in of itself (but other tasks should be scheduled instead)
    let barrier = new_start_barrier(2);
    let task_group = TaskGroup::new();
    sync_event(NewTaskGroup(task_group));
    tracing::debug!("sync_event starting join");
    sync_event(TaskWaitAnyN(task_group, 2));
    tracing::debug!("starting join");
    join!(
        task(
            "a",
            with_task_group(
                task_group,
                0,
                with_start_barrier(barrier.clone(), execution_point("a"))
            )
        ),
        task(
            "b",
            with_task_group(
                task_group,
                1,
                with_start_barrier(barrier.clone(), execution_point("b"))
            )
        )
    );
    tracing::debug!("joined");
    sync_event(TaskWaitCompleted);
    execution_point("joined").await;
    tracing::info!("ok");
    execution_point("select start").await;
    let barrier = new_start_barrier(2);
    let task_group = TaskGroup::new();
    sync_event(NewTaskGroup(task_group));
    tracing::debug!("sync_event starting select");
    sync_event(TaskWaitAnyN(task_group, 1));
    tracing::debug!("starting select");
    select! {
        _ = task("c", with_task_group(task_group, 0, with_start_barrier(barrier.clone(), execution_point("c")))).fuse() => {},
        _ = task("d", with_task_group(task_group, 1, with_start_barrier(barrier.clone(), execution_point("d")))).fuse() => {},
        _ = sleep(Duration::from_millis(1000)).fuse() => {}
    }
    tracing::debug!("done select");
    sync_event(TaskWaitCompleted);
    execution_point("selected").await;
}
