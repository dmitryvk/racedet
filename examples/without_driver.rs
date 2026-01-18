use std::{
    panic::AssertUnwindSafe,
    str::FromStr,
    sync::{Arc, atomic::AtomicI64},
    time::Duration,
};

use futures::FutureExt;
use racedet::{
    ReplayTrace, new_scheduler,
    task::{StartBarrier, Task, execution_point, task},
    with_scheduler,
};
use timeout_tracing::{CaptureSpanAndStackTrace, timeout};
use tokio::join;

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
        CaptureSpanAndStackTrace,
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
        Err(timeout) => {
            println!("timeout {}", timeout.active_traces[0].stack_trace());
        }
    }
    println!(
        "To replay this execution, set this environment variable:\nRACEDET_REPLAY=\"{}\"",
        scheduler.get_replay()
    );
    println!("{}", scheduler.get_trace());
}

async fn foo() {
    let var = Arc::new(AtomicI64::new(0));
    let barrier = StartBarrier::new(2);
    join!(
        task(
            Task::new("bar").with_start_barrier(barrier.clone()),
            bar(var.clone())
        ),
        task(
            Task::new("bar").with_start_barrier(barrier.clone()),
            bar(var.clone())
        ),
    );
    assert_eq!(2, var.load(std::sync::atomic::Ordering::Relaxed));
}

async fn bar(var: Arc<AtomicI64>) {
    execution_point("load").await;
    let x = var.load(std::sync::atomic::Ordering::Relaxed);
    execution_point("store").await;
    var.store(x + 1, std::sync::atomic::Ordering::Relaxed);
}
