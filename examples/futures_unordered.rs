use std::time::Duration;

use conc_checker::{
    driver::Driver,
    execution_point, new_start_barrier, sync_event, sync_init_event,
    sync_model::task_wait::{NewTaskGroup, TaskGroup, TaskWaitAnyN, TaskWaitCompleted},
    task, with_start_barrier, with_task_group,
};
use futures::StreamExt;
use futures::stream::FuturesUnordered;

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
    let task_group = TaskGroup::new();
    sync_init_event(NewTaskGroup(task_group));
    let barrier = new_start_barrier(3);
    let futs: FuturesUnordered<_> = (0..3)
        .map(|i| {
            let barrier = barrier.clone();
            async move {
                task(
                    format!("fut {i}"),
                    with_start_barrier(
                        barrier,
                        with_task_group(task_group, i, async {
                            execution_point("a").await;
                            i
                        }),
                    ),
                )
                .await
            }
        })
        .collect();
    let mut results: Vec<_> = task("main", async {
        tracing::debug!("sync_event starting collect");
        sync_event(TaskWaitAnyN(task_group, 3));
        tracing::debug!("starting collect");

        let res = futs.collect().await;
        tracing::debug!("joined");
        sync_event(TaskWaitCompleted);
        execution_point("joined").await;
        res
    })
    .await;
    results.sort();
    assert_eq!(results, (0..3).collect::<Vec<_>>());
}
