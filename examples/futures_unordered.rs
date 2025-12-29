use std::time::Duration;

use conc_checker::{
    capture_panics::capture_panic,
    execution_point, new_scheduler, register_task, sync_event,
    sync_model::join::{CompletedJoin, StartingJoin},
    task, with_scheduler,
};
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use timeout_tracing::{CaptureSpanAndStackTrace, timeout};

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
    let futs: FuturesUnordered<_> = (0..3)
        .map(|i| async move {
            task(register_task(&format!("fut {i}"), None), async {
                execution_point("a").await;
                i
            })
            .await
        })
        .collect();
    let mut results: Vec<_> = task(register_task("main", None), async {
        tracing::debug!("sync_event starting collect");
        sync_event(StartingJoin);
        tracing::debug!("starting collect");

        let res = futs.collect().await;
        tracing::debug!("joined");
        sync_event(CompletedJoin);
        execution_point("joined").await;
        res
    })
    .await;
    results.sort();
    assert_eq!(results, (0..3).collect::<Vec<_>>());
}
