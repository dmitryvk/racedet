use std::time::Duration;

use conc_checker::register_task_start_barrier;
use conc_checker::{execution_point, register_task, run_with_schedule, task, task_join};
use futures::FutureExt;
use futures::select;
use timeout_tracing::{CaptureSpanAndStackTrace, timeout};
use tokio::{join, time::sleep};

#[tokio::main]
async fn main() {
    match timeout(
        Duration::from_secs(1),
        CaptureSpanAndStackTrace,
        run_with_schedule(foo()),
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
    let task_a = register_task("a", start_barrier_1);
    let task_b = register_task("b", start_barrier_1);
    task_join("join", async {
        join!(
            task(task_a, execution_point("a")),
            task(task_b, execution_point("b"))
        )
    })
    .await;
    println!("ok");
    let start_barrier_2 = register_task_start_barrier("2", 3);
    let task_c = register_task("c", start_barrier_2);
    let task_d = register_task("d", start_barrier_2);
    let task_sleep = register_task("sleep", start_barrier_2);
    task_join("select", async {
        select! {
            _ = task(task_c, execution_point("c")).fuse() => {},
            _ = task(task_d, execution_point("d")).fuse() => {},
            _ = task(task_sleep, sleep(Duration::from_millis(10))).fuse() => {}
        }
    })
    .await;
}
