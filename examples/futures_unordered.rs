use std::{str::FromStr, time::Duration};

use conc_checker::{
    ReplayTrace,
    capture_panics::capture_panic,
    execution_point, new_scheduler, new_start_barrier, sync_event, sync_init_event,
    sync_model::task_wait::{NewTaskGroup, TaskGroup, TaskWaitAnyN, TaskWaitCompleted},
    task, with_scheduler, with_start_barrier, with_task_group,
};
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use timeout_tracing::{CaptureSpanAndStackTrace, timeout};

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
    let task_group = TaskGroup::new();
    sync_init_event(NewTaskGroup(task_group));
    let barrier = new_start_barrier(3);
    let futs: FuturesUnordered<_> = (0..3)
        .map(|i| {
            let barrier = barrier.clone();
            async move {
                task(
                    format!("fut {i}"),
                    with_start_barrier(
                        barrier,
                        with_task_group(task_group, i, async {
                            execution_point("a").await;
                            i
                        }),
                    ),
                )
                .await
            }
        })
        .collect();
    let mut results: Vec<_> = task("main", async {
        tracing::debug!("sync_event starting collect");
        sync_event(TaskWaitAnyN(task_group, 3));
        tracing::debug!("starting collect");

        let res = futs.collect().await;
        tracing::debug!("joined");
        sync_event(TaskWaitCompleted);
        execution_point("joined").await;
        res
    })
    .await;
    results.sort();
    assert_eq!(results, (0..3).collect::<Vec<_>>());
}
