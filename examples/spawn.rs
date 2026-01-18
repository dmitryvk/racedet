use std::time::Duration;

use racedet::{
    driver::Driver,
    sync_model::task_wait::{NewTaskGroup, TaskGroup, TaskWaitAnyN, TaskWaitCompleted},
    task::{StartBarrier, Task, execution_point, sync_event, task},
};
use tokio::{spawn, time::sleep};

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
    execution_point("before spawn").await;

    // task_join means that the current task is waiting for nested tasks and should not be scheduled in of itself (but other tasks should be scheduled instead)
    let barrier = StartBarrier::new(2);
    let task_group = TaskGroup::new();
    sync_event(NewTaskGroup(task_group));
    let task_a = spawn(task(
        Task::new("spawn a")
            .with_task_group(task_group, 0)
            .with_start_barrier(barrier.clone()),
        baz(1),
    ));
    let task_b = spawn(task(
        Task::new("spawn b")
            .with_task_group(task_group, 1)
            .with_start_barrier(barrier.clone()),
        baz(2),
    ));
    // TODO: sleep is a hack to ensure that spawn happens before the task becomes blocked
    sleep(Duration::from_millis(1)).await;
    tracing::debug!("sync_event starting join");
    sync_event(TaskWaitAnyN(task_group, 2));
    tracing::debug!("starting join");
    task_a.await.unwrap();
    task_b.await.unwrap();
    tracing::debug!("joined");
    sync_event(TaskWaitCompleted);
    tracing::info!("ok");
    execution_point("joined").await;
}

async fn baz(n: u32) {
    execution_point("a1").await;
    let task_group = TaskGroup::new();
    sync_event(NewTaskGroup(task_group));
    let r = spawn(task(
        Task::new(format!("spawn baz {n}")).with_task_group(task_group, 0),
        execution_point("q"),
    ));
    // TODO: call TaskWaitAnyN before spawn
    // TODO: TaskWaitAnyN introduces non-determinism if a child task is spawned when the current task is "blocked"
    //       this can be solved with "spawn promises"
    // TODO: sleep is a hack to ensure that spawn happens before the task becomes blocked
    sleep(Duration::from_millis(1)).await;
    sync_event(TaskWaitAnyN(task_group, 1));
    r.await.unwrap();
    sync_event(TaskWaitCompleted);
    execution_point("a2").await;
}
