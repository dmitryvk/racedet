use std::{
    sync::{Arc, atomic::AtomicI64},
    time::Duration,
};

use racedet::{
    driver::Driver,
    task::{StartBarrier, Task, execution_point, task},
};
use tokio::join;

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
    let var = Arc::new(AtomicI64::new(0));
    let barrier = StartBarrier::new(2);
    join!(
        task(
            Task::new("bar").with_start_barrier(barrier.clone()),
            bar(var.clone())
        ),
        task(
            Task::new("bar").with_start_barrier(barrier.clone()),
            bar(var.clone())
        )
    );
    let result = var.load(std::sync::atomic::Ordering::Relaxed);
    assert!(result == 1 || result == 2);
}

async fn bar(var: Arc<AtomicI64>) {
    execution_point("load").await;
    let x = var.load(std::sync::atomic::Ordering::Relaxed);
    execution_point("store").await;
    var.store(x + 1, std::sync::atomic::Ordering::Relaxed);
}
