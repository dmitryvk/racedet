use std::{sync::Arc, time::Duration};

use racedet::{
    driver::Driver,
    sync_model::mutex::{LockedMutex, LockingMutex, MutexId, ReleasedMutex},
    task::{StartBarrier, Task, execution_point, sync_event, task},
};
use tokio::{join, sync::Mutex};

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();
    Driver::new()
        .with_replay_many_env_var("RACE_DET_REPLAY")
        .max_iterations_env_var("RACE_DET_ITERS", None)
        .max_total_duration_env_var("RACE_DET_DURATION_SEC", Duration::from_secs(10))
        .test_timeout(Duration::from_secs(1))
        .run_async(async || foo().await)
        .await;
}

async fn foo() {
    execution_point("before").await;

    let barrier = StartBarrier::new(2);

    let var = Arc::new(Mutex::new(1));
    let mutex_id = MutexId::new("m".to_string());

    join!(
        task(
            Task::new("inc1").with_start_barrier(barrier.clone()),
            async { do_inc(var.clone(), mutex_id.clone()).await }
        ),
        task(
            Task::new("inc2").with_start_barrier(barrier.clone()),
            async { do_inc(var.clone(), mutex_id.clone()).await }
        ),
    );

    // Due to a race between reading of value and incrementing it, the value may be actually either 2 or 3
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
