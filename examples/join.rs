use std::time::Duration;

use futures::FutureExt;
use racedet::{
    driver::Driver,
    sync_model::task_wait::{NewTaskGroup, TaskGroup, TaskWaitAnyN, TaskWaitCompleted},
    task::{StartBarrier, Task, execution_point, sync_event, task},
};
use tokio::{join, select, time::sleep};

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
    task(Task::new("bar"), bar()).await;
}

async fn bar() {
    execution_point("spawn tasks").await;

    // task_join means that the current task is waiting for nested tasks
    // and should not be scheduled in of itself (but other tasks should be scheduled instead)
    let barrier = StartBarrier::new(2);
    let task_group = TaskGroup::new();
    sync_event(NewTaskGroup(task_group));
    tracing::debug!("sync_event starting join");
    sync_event(TaskWaitAnyN(task_group, 2));
    tracing::debug!("starting join");
    join!(
        task(
            Task::new("a")
                .with_task_group(task_group, 0)
                .with_start_barrier(barrier.clone()),
            async {
                execution_point("a").await;
            }
        ),
        task(
            Task::new("b")
                .with_task_group(task_group, 1)
                .with_start_barrier(barrier.clone()),
            async {
                execution_point("b").await;
            }
        ),
    );
    tracing::debug!("joined");
    sync_event(TaskWaitCompleted);
    execution_point("joined").await;
    tracing::info!("ok");
    execution_point("select start").await;
    let barrier = StartBarrier::new(2);
    let task_group = TaskGroup::new();
    sync_event(NewTaskGroup(task_group));
    tracing::debug!("sync_event starting select");
    sync_event(TaskWaitAnyN(task_group, 1));
    tracing::debug!("starting select");
    let task_c = task(
        Task::new("c")
            .with_task_group(task_group, 0)
            .with_start_barrier(barrier.clone()),
        async {
            execution_point("c").await;
        },
    );
    let task_d = task(
        Task::new("d")
            .with_task_group(task_group, 1)
            .with_start_barrier(barrier.clone()),
        async { execution_point("d").await },
    );
    select! {
        _ = task_c => {},
        _ = task_d => {},
        _ = sleep(Duration::from_millis(1000)).fuse() => {}
    }
    tracing::debug!("done select");
    sync_event(TaskWaitCompleted);
    execution_point("selected").await;
}
