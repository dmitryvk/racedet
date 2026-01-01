use std::time::Duration;

use conc_checker::capture_panics::capture_panic;
use conc_checker::sync_model::join::{CompletedJoin, StartingJoin};
use conc_checker::{execution_point, new_start_barrier, task, with_start_barrier};
use conc_checker::{new_scheduler, sync_event, with_scheduler};
use futures::FutureExt;
use futures::select;
use timeout_tracing::{CaptureSpanAndStackTrace, timeout};
use tokio::{join, time::sleep};

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();
    let replay = std::env::var("CONC_CHECKER_REPLAY").ok();
    let (scheduler, control_fut) = new_scheduler(replay.as_deref());
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
    println!("{}", scheduler.get_replay());
}

async fn foo() {
    task("bar", bar()).await;
}

async fn bar() {
    execution_point("spawn tasks").await;

    // task_join means that the current task is waiting for nested tasks and should not be scheduled in of itself (but other tasks should be scheduled instead)
    let barrier = new_start_barrier(2);
    tracing::debug!("sync_event starting join");
    sync_event(StartingJoin);
    tracing::debug!("starting join");
    join!(
        task(
            "a",
            with_start_barrier(barrier.clone(), execution_point("a"))
        ),
        task(
            "b",
            with_start_barrier(barrier.clone(), execution_point("b"))
        )
    );
    tracing::debug!("joined");
    sync_event(CompletedJoin);
    execution_point("joined").await;
    tracing::info!("ok");
    execution_point("select start").await;
    let barrier = new_start_barrier(3);
    tracing::debug!("sync_event starting select");
    sync_event(StartingJoin);
    tracing::debug!("starting select");
    select! {
        _ = task("c", with_start_barrier(barrier.clone(), execution_point("c"))).fuse() => {},
        _ = task("d", with_start_barrier(barrier.clone(), execution_point("d"))).fuse() => {},
        _ = task("sleep", with_start_barrier(barrier.clone(), sleep(Duration::from_millis(10)))).fuse() => {}
    }
    tracing::debug!("done select");
    sync_event(CompletedJoin);
    execution_point("selected").await;
}
