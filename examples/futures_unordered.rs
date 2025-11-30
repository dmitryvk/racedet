use std::time::Duration;

use conc_checker::{execute, new_scheduler};
use conc_checker::{execution_point, register_task, task, task_join};
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use timeout_tracing::{CaptureSpanAndStackTrace, timeout};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

#[tokio::main]
async fn main() {
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer())
        .with(tracing_subscriber::EnvFilter::from_default_env())
        .init();
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
    let futs: FuturesUnordered<_> = (0..512)
        .map(|i| async move {
            task(register_task(&format!("fut {i}"), None), async {
                execution_point("a").await;
                i
            })
            .await
        })
        .collect();
    let mut results: Vec<_> = task(
        register_task("main", None),
        task_join("fut_unordered_collect", futs.collect()),
    )
    .await;
    results.sort();
    assert_eq!(results, (0..512).collect::<Vec<_>>());
}
