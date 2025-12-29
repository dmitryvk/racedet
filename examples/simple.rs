use std::{
    sync::{Arc, atomic::AtomicI64},
    time::Duration,
};

use conc_checker::{
    capture_panics::capture_panic, execution_point, new_scheduler, register_task,
    register_task_start_barrier, task, with_scheduler,
};
use timeout_tracing::{CaptureSpanAndStackTrace, timeout};
use tokio::join;

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
    let var = Arc::new(AtomicI64::new(0));
    let start_barrier = register_task_start_barrier("tasks", 2);
    let bar1 = register_task("bar", start_barrier);
    let bar2 = register_task("bar", start_barrier);
    join!(task(bar1, bar(var.clone())), task(bar2, bar(var.clone())));
    assert_eq!(2, var.load(std::sync::atomic::Ordering::Relaxed));
}

async fn bar(var: Arc<AtomicI64>) {
    execution_point("load").await;
    let x = var.load(std::sync::atomic::Ordering::Relaxed);
    execution_point("store").await;
    var.store(x + 1, std::sync::atomic::Ordering::Relaxed);
}
