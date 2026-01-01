use std::sync::Arc;
use std::time::Duration;

use conc_checker::capture_panics::capture_panic;
use conc_checker::sync_model::rwlock::{
    LockedRwlock, LockingRwlock, ReleasedRwlock, RwlockId, RwlockMode,
};
use conc_checker::{execution_point, new_start_barrier, task, with_start_barrier};
use conc_checker::{new_scheduler, sync_event, with_scheduler};
use timeout_tracing::{CaptureSpanAndStackTrace, timeout};
use tokio::join;
use tokio::sync::RwLock;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();
    let replay = std::env::var("CONC_CHECKER_REPLAY").ok();
    let (scheduler, control_fut) = new_scheduler(replay.as_deref());
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
    println!("{}", scheduler.get_replay());
}

async fn foo() {
    execution_point("before").await;

    let barrier = new_start_barrier(4);

    let var = Arc::new(RwLock::new(1));
    let rwlock_id = RwlockId::new("rwlock".to_string());

    join!(
        task(
            "r1",
            with_start_barrier(barrier.clone(), do_read(var.clone(), rwlock_id.clone()))
        ),
        task(
            "r2",
            with_start_barrier(barrier.clone(), do_read(var.clone(), rwlock_id.clone()))
        ),
        task(
            "w1",
            with_start_barrier(barrier.clone(), do_write(var.clone(), rwlock_id.clone()))
        ),
        task(
            "w2",
            with_start_barrier(barrier.clone(), do_write(var.clone(), rwlock_id.clone()))
        ),
    );

    assert_eq!(3, *var.read().await);
}

async fn do_read(var: Arc<RwLock<i32>>, rwlock_id: RwlockId) {
    tracing::debug!("sync_event: locking for read");
    sync_event(LockingRwlock(rwlock_id.clone(), RwlockMode::Read));
    tracing::debug!("locking for read");
    execution_point("take read-lock").await;
    let guard = var.read().await;
    sync_event(LockedRwlock(rwlock_id.clone(), RwlockMode::Read));
    execution_point("read var").await;
    let _value = *guard;

    execution_point("release read-lock").await;
    sync_event(ReleasedRwlock(rwlock_id.clone(), RwlockMode::Read));
    tracing::debug!("unlocked for read");
    drop(guard);
}

async fn do_write(var: Arc<RwLock<i32>>, rwlock_id: RwlockId) {
    tracing::debug!("sync_event: locking for write");
    sync_event(LockingRwlock(rwlock_id.clone(), RwlockMode::Write));
    tracing::debug!("locking for write");
    execution_point("take write-lock").await;
    let mut guard = var.write().await;
    sync_event(LockedRwlock(rwlock_id.clone(), RwlockMode::Write));
    execution_point("modify var").await;
    let val = &mut *guard;
    *val += 1;
    execution_point("release write-lock").await;
    sync_event(ReleasedRwlock(rwlock_id.clone(), RwlockMode::Write));
    tracing::debug!("unlocked for write");
    drop(guard);
}
