use std::sync::Arc;
use std::time::Duration;

use conc_checker::capture_panics::capture_panic;
use conc_checker::sync_model::mutex::{LockedMutex, LockingMutex, MutexId, ReleasedMutex};
use conc_checker::{execution_point, register_task, task};
use conc_checker::{new_scheduler, register_task_start_barrier, sync_event, with_scheduler};
use timeout_tracing::{CaptureSpanAndStackTrace, timeout};
use tokio::join;
use tokio::sync::Mutex;

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
    execution_point("before").await;

    let barrier = register_task_start_barrier("start", 2);

    let var = Arc::new(Mutex::new(1));
    let mutex_id = MutexId::new("m".to_string());

    join!(
        task(
            register_task("inc1", barrier),
            do_inc(var.clone(), mutex_id.clone())
        ),
        task(
            register_task("inc2", barrier),
            do_inc(var.clone(), mutex_id.clone())
        ),
    );

    assert_eq!(3, *var.lock().await);
}

async fn do_inc(var: Arc<Mutex<i32>>, mutex_id: MutexId) {
    tracing::debug!("sync_event: locking mutex");
    sync_event(LockingMutex(mutex_id.clone()));
    execution_point("lock mutex for read").await;
    tracing::debug!("locking mutex");
    let guard = var.lock().await;
    tracing::debug!("locked mutex");
    sync_event(LockedMutex(mutex_id.clone()));
    execution_point("read value").await;
    let value = *guard;
    execution_point("unlock mutex for read").await;
    drop(guard);
    tracing::debug!("unlocked mutex");
    sync_event(ReleasedMutex(mutex_id.clone()));

    tracing::debug!("sync_event: locking mutex");
    sync_event(LockingMutex(mutex_id.clone()));
    execution_point("lock mutex for write").await;
    tracing::debug!("locking mutex");
    let mut guard = var.lock().await;
    tracing::debug!("locked mutex");
    sync_event(LockedMutex(mutex_id.clone()));
    execution_point("write value").await;
    *guard = value + 1;
    execution_point("unlock mutex for write").await;
    drop(guard);
    tracing::debug!("unlocked mutex");
    sync_event(ReleasedMutex(mutex_id.clone()));
}
