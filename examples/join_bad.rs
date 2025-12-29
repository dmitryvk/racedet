use std::time::Duration;

use conc_checker::{
    capture_panics::capture_panic, execution_point, new_scheduler, register_task, task,
    with_scheduler,
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
    task(register_task("bar", None), bar()).await;
}

async fn bar() {
    execution_point("before").await;
    join!(execution_point("a"), execution_point("b"));
}
