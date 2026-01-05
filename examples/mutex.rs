use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use conc_checker::capture_panics::capture_panic;
use conc_checker::sync_model::mutex::{LockedMutex, LockingMutex, MutexId, ReleasedMutex};
use conc_checker::{ReplayTrace, execution_point, new_start_barrier, task, with_start_barrier};
use conc_checker::{new_scheduler, sync_event, with_scheduler};
use timeout_tracing::{CaptureSpanAndStackTrace, timeout};
use tokio::join;
use tokio::sync::Mutex;

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
                "panic {} at {}\n{}",
                panic.message, panic.location, panic.backtrace
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
    execution_point("before").await;

    let barrier = new_start_barrier(2);

    let var = Arc::new(Mutex::new(1));
    let mutex_id = MutexId::new("m".to_string());

    join!(
        task(
            "inc1",
            with_start_barrier(barrier.clone(), do_inc(var.clone(), mutex_id.clone()))
        ),
        task(
            "inc2",
            with_start_barrier(barrier.clone(), do_inc(var.clone(), mutex_id.clone()))
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
