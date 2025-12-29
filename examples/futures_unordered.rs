use std::time::Duration;

use conc_checker::sync_model::join::{CompletedJoin, StartingJoin};
use conc_checker::{execute, new_scheduler, sync_event};
use conc_checker::{execution_point, register_task, task};
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use timeout_tracing::{CaptureSpanAndStackTrace, timeout};

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();
    match timeout(
        Duration::from_secs(10),
        CaptureSpanAndStackTrace,
        execute(new_scheduler(), foo()),
    )
    .await
    {
        Ok((trace, res)) => {
            println!("{res:?}");
            println!("{trace}");
        }
        Err(err) => {
            println!("timeout {}", err.active_traces[0].stack_trace());
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
