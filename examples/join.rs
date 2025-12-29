use std::time::Duration;

use conc_checker::capture_panics::capture_panic;
use conc_checker::sync_model::join::{CompletedJoin, StartingJoin};
use conc_checker::{execution_point, register_task, task};
use conc_checker::{new_scheduler, register_task_start_barrier, sync_event, with_scheduler};
use futures::FutureExt;
use futures::select;
use timeout_tracing::{CaptureSpanAndStackTrace, timeout};
use tokio::{join, time::sleep};

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
    execution_point("spawn tasks").await;

    // task_join means that the current task is waiting for nested tasks and should not be scheduled in of itself (but other tasks should be scheduled instead)
    let start_barrier_1 = register_task_start_barrier("1", 2);
    let task_a = register_task("a", start_barrier_1);
    let task_b = register_task("b", start_barrier_1);
    tracing::debug!("sync_event starting join");
    sync_event(StartingJoin);
    tracing::debug!("starting join");
    join!(
        task(task_a, execution_point("a")),
        task(task_b, execution_point("b"))
    );
    tracing::debug!("joined");
    sync_event(CompletedJoin);
    execution_point("joined").await;
    tracing::info!("ok");
    execution_point("select start").await;
    let start_barrier_2 = register_task_start_barrier("2", 3);
    let task_c = register_task("c", start_barrier_2);
    let task_d = register_task("d", start_barrier_2);
    let task_sleep = register_task("sleep", start_barrier_2);
    tracing::debug!("sync_event starting select");
    sync_event(StartingJoin);
    tracing::debug!("starting select");
    select! {
        _ = task(task_c, execution_point("c")).fuse() => {},
        _ = task(task_d, execution_point("d")).fuse() => {},
        _ = task(task_sleep, sleep(Duration::from_millis(10))).fuse() => {}
    }
    tracing::debug!("done select");
    sync_event(CompletedJoin);
    execution_point("selected").await;
}
