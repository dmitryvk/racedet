use std::time::Duration;

use conc_checker::{driver::Driver, execution_point, task};
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
    task("bar", bar()).await;
}

async fn bar() {
    execution_point("before").await;
    // This fails with a message:
    // task 1 1 reached point 4, but its state is not Running, but rather is Suspended { point: StringIdx(3) }.
    // This might mean that an internal task concurrency is happening (e.g., join or FuturesUnordered).
    join!(execution_point("a"), execution_point("b"));
}
