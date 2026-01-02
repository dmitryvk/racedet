use std::time::Duration;

use conc_checker::{
    capture_panics::capture_panic,
    current_scheduler, execution_point, maybe_with_scheduler, new_scheduler, new_start_barrier,
    sync_event,
    sync_model::join::{CompletedJoin, StartingJoin},
    task, with_scheduler, with_start_barrier,
};
use timeout_tracing::{CaptureSpanAndStackTrace, timeout};
use tokio::spawn;

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
    execution_point("before spawn").await;

    // task_join means that the current task is waiting for nested tasks and should not be scheduled in of itself (but other tasks should be scheduled instead)
    let barrier = new_start_barrier(2);
    let task_a = spawn(maybe_with_scheduler(
        current_scheduler(),
        task("spawn a", with_start_barrier(barrier.clone(), baz(1))),
    ));
    let task_b = spawn(maybe_with_scheduler(
        current_scheduler(),
        task("spawn b", with_start_barrier(barrier.clone(), baz(2))),
    ));
    tracing::debug!("sync_event starting join");
    sync_event(StartingJoin);
    tracing::debug!("starting join");
    task_a.await.unwrap();
    task_b.await.unwrap();
    tracing::debug!("joined");
    sync_event(CompletedJoin);
    tracing::info!("ok");
    execution_point("joined").await;
}

async fn baz(n: u32) {
    execution_point("a1").await;
    sync_event(StartingJoin);
    spawn(maybe_with_scheduler(
        current_scheduler(),
        // TODO: when multiple tasks finish concurrently,
        // there is a race between completion of one task and `CompletedJoin` of another task
        task(format!("spawn baz {n}"), execution_point("q")),
    ))
    .await
    .unwrap();
    sync_event(CompletedJoin);
    execution_point("a2").await;
}
