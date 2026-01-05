use std::{
    str::FromStr,
    sync::{Arc, atomic::AtomicI64},
    time::Duration,
};

use conc_checker::{
    ReplayTrace, capture_panics::capture_panic, execution_point, new_scheduler, new_start_barrier,
    task, with_scheduler, with_start_barrier,
};
use timeout_tracing::{CaptureSpanAndStackTrace, timeout};
use tokio::join;

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
                "panic at {location}:\n{message}\n{backtrace}",
                message = panic.message,
                location = panic.location,
                backtrace = panic.backtrace,
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
    let var = Arc::new(AtomicI64::new(0));
    let barrier = new_start_barrier(2);
    join!(
        task("bar", with_start_barrier(barrier.clone(), bar(var.clone()))),
        task("bar", with_start_barrier(barrier.clone(), bar(var.clone())))
    );
    assert_eq!(2, var.load(std::sync::atomic::Ordering::Relaxed));
}

async fn bar(var: Arc<AtomicI64>) {
    execution_point("load").await;
    let x = var.load(std::sync::atomic::Ordering::Relaxed);
    execution_point("store").await;
    var.store(x + 1, std::sync::atomic::Ordering::Relaxed);
}
