use std::sync::Arc;
use std::time::Duration;

use conc_checker::{
    RunResult, execute, execution_point_with_locks, new_scheduler, register_task_start_barrier,
};
use conc_checker::{execution_point, register_task, task};
use timeout_tracing::{CaptureSpanAndStackTrace, timeout};
use tokio::join;
use tokio::sync::RwLock;

#[tokio::main]
async fn main() {
    let (trace, res) = execute(
        new_scheduler(),
        timeout(Duration::from_secs(1), CaptureSpanAndStackTrace, foo()),
    )
    .await;
    println!("{trace}");
    match res {
        RunResult::Ok(Ok(res)) => {
            println!("{res:?}");
        }
        RunResult::Ok(Err(err)) => {
            println!("timeout {}", err.active_traces[0].stack_trace());
        }
        RunResult::Panic(pi) => {
            println!("panic {} at {}\n{}", pi.message, pi.location, pi.backtrace);
        }
    }
}

async fn foo() {
    execution_point("before").await;

    let barrier = register_task_start_barrier("start", 3);

    let var = Arc::new(RwLock::new(1));

    join!(
        task(register_task("task 1", barrier), do_read(var.clone())),
        task(register_task("task 2", barrier), do_write(var.clone())),
        task(register_task("task 3", barrier), do_write(var.clone())),
    );

    assert_eq!(3, *var.read().await);
}

async fn do_read(var: Arc<RwLock<i32>>) {
    execution_point_with_locks("before read-lock", vec!["var".to_string()], &[]).await;
    let guard = var.read().await;
    execution_point("read-locked").await;
    drop(guard);
    execution_point_with_locks("unlocked", vec![], &["var".to_string()]).await;
}

async fn do_write(var: Arc<RwLock<i32>>) {
    execution_point_with_locks("before write-lock", vec!["var".to_string()], &[]).await;
    let mut guard = var.write().await;
    execution_point("write-locked").await;
    let val = &mut *guard;
    *val += 1;
    drop(guard);
    execution_point_with_locks("unlocked", vec![], &["var".to_string()]).await;
}
