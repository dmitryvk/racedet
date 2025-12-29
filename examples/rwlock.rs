use std::sync::Arc;
use std::time::Duration;

use conc_checker::sync_model::rwlock::{
    LockedRwlock, LockingRwlock, ReleasedRwlock, RwlockId, RwlockMode,
};
use conc_checker::{RunResult, execute, new_scheduler, register_task_start_barrier, sync_event};
use conc_checker::{execution_point, register_task, task};
use timeout_tracing::{CaptureSpanAndStackTrace, timeout};
use tokio::join;
use tokio::sync::RwLock;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();
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

    let barrier = register_task_start_barrier("start", 4);

    let var = Arc::new(RwLock::new(1));
    let rwlock_id = RwlockId::new("rwlock".to_string());

    join!(
        task(
            register_task("r1", barrier),
            do_read(var.clone(), rwlock_id.clone())
        ),
        task(
            register_task("r2", barrier),
            do_read(var.clone(), rwlock_id.clone())
        ),
        task(
            register_task("w1", barrier),
            do_write(var.clone(), rwlock_id.clone())
        ),
        task(
            register_task("w2", barrier),
            do_write(var.clone(), rwlock_id.clone())
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
