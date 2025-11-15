use std::time::Duration;

use conc_checker::{
    current_scheduler, execute, maybe_with_scheduler, new_scheduler, register_task_start_barrier,
};
use conc_checker::{execution_point, register_task, task, task_join};
use futures::FutureExt;
use futures::select;
use timeout_tracing::{CaptureSpanAndStackTrace, timeout};
use tokio::spawn;
use tokio::{join, time::sleep};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

#[tokio::main]
async fn main() {
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer())
        .with(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    match timeout(
        Duration::from_secs(1),
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
    task(register_task("bar", None), bar()).await;
}

async fn bar() {
    execution_point("before").await;

    // task_join means that the current task is waiting for nested tasks and should not be scheduled in of itself (but other tasks should be scheduled instead)
    let start_barrier_1 = register_task_start_barrier("1", 2);
    let task_a = register_task("spawn a", start_barrier_1);
    let task_b = register_task("spawn b", start_barrier_1);
    task_join("join", async {
        let (ra, rb) = join!(
            spawn(maybe_with_scheduler(
                current_scheduler(),
                task(task_a, execution_point("a"))
            )),
            spawn(maybe_with_scheduler(
                current_scheduler(),
                task(task_b, execution_point("b"))
            )),
        );
        ra.unwrap();
        rb.unwrap();
    })
    .await;
    println!("ok");
}
