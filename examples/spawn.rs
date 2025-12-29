use std::time::Duration;

use conc_checker::{
    capture_panics::capture_panic,
    current_scheduler, execution_point, maybe_with_scheduler, new_scheduler, register_task,
    register_task_start_barrier, sync_event,
    sync_model::join::{CompletedJoin, StartingJoin},
    task, with_scheduler,
};
use timeout_tracing::{CaptureSpanAndStackTrace, timeout};
use tokio::spawn;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();
    let (scheduler, control_fut) = new_scheduler();
    tokio::spawn(control_fut);
    let res = timeout(
        Duration::from_secs(10),
        CaptureSpanAndStackTrace,
        with_scheduler(scheduler.clone(), capture_panic(foo())),
    )
    .await;
    let trace = scheduler.get_trace();
    println!("{trace}");
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
}

async fn foo() {
    task(register_task("bar", None), bar()).await;
}

async fn bar() {
    execution_point("before spawn").await;

    // task_join means that the current task is waiting for nested tasks and should not be scheduled in of itself (but other tasks should be scheduled instead)
    let start_barrier_1 = register_task_start_barrier("1", 2);
    let task_a = spawn(maybe_with_scheduler(
        current_scheduler(),
        task(
            register_task("spawn a", start_barrier_1),
            execution_point("a"),
        ),
    ));
    let task_b = spawn(maybe_with_scheduler(
        current_scheduler(),
        task(
            register_task("spawn b", start_barrier_1),
            execution_point("b"),
        ),
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
